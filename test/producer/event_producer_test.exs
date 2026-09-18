# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.Test do
  use ExUnit.Case
  use Mimic

  alias AMQP.Basic
  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Queue
  alias Mississippi.Producer.EventsProducer
  alias Mississippi.Producer.EventsProducer.AMQPConnection
  alias Mississippi.Producer.EventsProducer.State
  alias Mississippi.Producer.EventsProducer.Worker
  alias Mississippi.Producer.ProducersSupervisor

  require Logger

  @moduletag :unit

  setup do
    Mimic.set_mimic_global()
    AMQPConnection |> stub(:init, fn _, _ -> {:ok, channel_fixture()} end)
    Queue |> stub(:declare, fn _, _, _ -> {:ok, nil} end)

    total_count = 8

    events_producer_supervisor_pid =
      start_supervised!(events_producer_supervisor_fixture(total_count))

    producers = event_producer_pids(total_count)

    mississippi_config = [queues: [total_count: total_count]]

    %{
      events_producer_supervisor_pid: events_producer_supervisor_pid,
      mississippi_config: mississippi_config,
      producers: producers,
      total_count: total_count
    }
  end

  describe "EventsProducer.publish/3 via router" do
    @tag :events_producer_message_handling
    test "publishes the message to the correct shard worker when connected", context do
      %{total_count: total_count, mississippi_config: mississippi_config} = context
      sharding_key = 42
      expected_index = :erlang.phash2(sharding_key, total_count)
      expected_queue = "#{expected_index}"

      valid_payload = "payload-#{System.unique_integer([:positive])}"
      valid_opts = [sharding_key: sharding_key]

      expect(Basic, :publish, fn _, _, ^expected_queue, ^valid_payload, _ ->
        :ok
      end)

      assert :ok == EventsProducer.publish(valid_payload, valid_opts, mississippi_config)
    end

    @tag :events_producer_message_handling
    test "returns an error when it fails to connect to a channel", context do
      %{mississippi_config: mississippi_config, producers: producers} =
        context

      test_process = self()
      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()
      sharding_key = valid_opts[:sharding_key]
      {index, producer} = producer_for_sharding_key(producers, sharding_key)

      AMQPConnection
      |> stub(:init, fn _, _ ->
        send(test_process, :reconnecting)
        {:error, :event_producer_init_fail}
      end)

      Process.exit(producer, :kill)
      assert_receive :reconnecting
      _ = event_producer_pid(index)

      assert {:error, :reconnecting} ==
               EventsProducer.publish(valid_payload, valid_opts, mississippi_config)
    end

    @tag :events_producer_message_handling
    test "returns an error when reconnecting", context do
      %{producers: producers, mississippi_config: mississippi_config} = context
      valid_payload = payload_fixture()
      valid_opts = publish_options_fixture()

      sharding_key = valid_opts[:sharding_key]
      {queue_index, producer} = producer_for_sharding_key(producers, sharding_key)
      %State{channel: %{pid: channel_pid}} = :sys.get_state(producer)
      test_process = self()

      AMQPConnection
      |> stub(:init, fn _, _ ->
        send(test_process, :channel_init)
        {:error, :event_producer_init_fail}
      end)

      Process.exit(channel_pid, :kill)
      assert_receive :channel_init

      new_producer_pid = event_producer_pid(queue_index, 800)

      %State{channel: nil} = :sys.get_state(new_producer_pid)

      assert {:error, :reconnecting} ==
               EventsProducer.publish(valid_payload, valid_opts, mississippi_config)
    end
  end

  @tag :events_producer_fault_tolerance
  test "reconnects if the AMQP connection goes down", context do
    %{producers: producers} = context
    test_process = self()

    AMQPConnection
    |> stub(:init, fn _, _ ->
      send(test_process, :channel_init)
      {:error, :event_producer_init_fail}
    end)

    {index, producer_pid} = Enum.random(producers)
    %State{channel: %{pid: channel_pid}} = :sys.get_state(producer_pid)

    Process.exit(channel_pid, :kill)
    assert_receive :channel_init

    producer_pid = event_producer_pid(index)

    assert %State{channel: nil} = :sys.get_state(producer_pid)

    reconnected_message = :events_producer_reconnected

    AMQPConnection
    |> expect(:init, fn _, _ ->
      # send a message to the test process to signal that
      # the events producer tried to (re)initialize the connection
      send(test_process, reconnected_message)

      {:ok, channel_fixture(test_process)}
    end)

    assert_receive ^reconnected_message

    assert %State{channel: %{pid: ^test_process}} = :sys.get_state(producer_pid)
  end

  describe "EventsProducer's initialization" do
    @tag :events_producer_initialization
    test "fails when the sharding key is not specified", %{mississippi_config: mississippi_config} do
      valid_payload = "payload-#{System.unique_integer([:positive])}"

      assert_raise NimbleOptions.ValidationError, fn ->
        EventsProducer.publish(valid_payload, [], mississippi_config)
      end
    end

    @tag :events_producer_initialization
    test "fails when total_count is not an integer", %{mississippi_config: mississippi_config} do
      valid_payload = "payload-#{System.unique_integer([:positive])}"
      invalid_config = put_in(mississippi_config, [:queues, :total_count], "not_an_integer")

      assert_raise NimbleOptions.ValidationError, fn ->
        EventsProducer.publish(valid_payload, [sharding_key: 123], invalid_config)
      end
    end
  end

  defp loop do
    receive do
      _ -> loop()
    end
  end

  defp channel_fixture(channel_pid \\ nil, connection_pid \\ nil) do
    channel_pid = channel_pid || spawn(&loop/0)
    connection_pid = connection_pid || self()

    %Channel{
      pid: channel_pid,
      conn: %Connection{pid: connection_pid}
    }
  end

  defp payload_fixture do
    "payload-#{System.unique_integer([:positive])}"
  end

  defp publish_options_fixture do
    [sharding_key: System.unique_integer()]
  end

  defp events_producer_supervisor_fixture(total_count) do
    opts = [
      mississippi_config: [
        queues: [
          total_count: total_count,
          ssl_options: [verify: :verify_none],
          events_exchange_name: "mississippi_#{System.unique_integer([:positive])}",
          total_count: System.unique_integer([:positive]),
          connection_options: [host: "localhost"],
          reconnection_backoff_ms: 0
        ]
      ],
      amqp_producer_options: [host: "localhost"]
    ]

    {ProducersSupervisor, opts}
  end

  defp event_producer_pids(total_count) do
    last_index = total_count - 1

    0..last_index
    |> Enum.map(&{&1, event_producer_pid(&1)})
  end

  def event_producer_pid(queue_index, retries \\ 10) do
    id = Worker.via_tuple(queue_index)

    case {retries, GenServer.whereis(id)} do
      {0, nil} ->
        flunk("Event producer with index #{queue_index} did not start")

      {n, nil} ->
        Logger.debug("Event producer with index #{queue_index}: start failed at retry #{n}")
        Process.sleep(20)
        event_producer_pid(queue_index, n - 1)

      {_, pid} when is_pid(pid) ->
        pid

      {_, other} ->
        flunk("Expected pid but got #{inspect(other)} for queue with index #{queue_index}")
    end
  end

  defp producer_for_sharding_key(producers, sharding_key) do
    total_count = Enum.count(producers)
    index = :erlang.phash2(sharding_key, total_count)
    Enum.find_value(producers, fn {id, pid} -> id == index && {index, pid} end)
  end
end
