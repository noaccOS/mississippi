# Copyright 2025 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer.Starter do
  @moduledoc false
  use Task, restart: :transient

  alias Horde.DynamicSupervisor
  alias Mississippi.Producer.EventsProducer.Worker

  require Logger

  @restart_backoff :timer.seconds(2)

  def start_link(queues_config) do
    Task.start_link(__MODULE__, :start_producers, [queues_config])
  end

  def start_producers(queues_config) do
    start_producers(queues_config, 10)
  end

  defp start_producers(_, 0) do
    _ = Logger.warning("Cannot start EventsProducers")
    {:error, :cannot_start_producers}
  end

  defp start_producers(queues_config, retry) do
    start_amqp_producers(queues_config)

    queue_total = queues_config[:total_count]

    child_count =
      Worker.Supervisor |> DynamicSupervisor.which_children() |> Enum.count()

    case child_count do
      ^queue_total ->
        :ok

      _ ->
        backoff_delta = :rand.uniform(@restart_backoff)
        Process.sleep(@restart_backoff + backoff_delta)
        start_producers(queues_config, retry - 1)
    end
  end

  def start_amqp_producers(queues_config) do
    children = amqp_producers_childspecs(queues_config)

    Enum.each(children, fn child ->
      DynamicSupervisor.start_child(Worker.Supervisor, child)
    end)
  end

  defp amqp_producers_childspecs(queues_config) do
    queue_prefix = queues_config[:prefix]
    queue_total = queues_config[:total_count]
    events_exchange_name = queues_config[:events_exchange_name]
    connection_options = queues_config[:connection_options]
    reconnection_backoff_ms = Keyword.get(queues_config, :reconnection_backoff_ms, 1_000)

    max_index = queue_total - 1

    for queue_index <- 0..max_index do
      routing_key = "#{queue_prefix}#{queue_index}"

      init_args = [
        routing_key: routing_key,
        queue_index: queue_index,
        events_exchange_name: events_exchange_name,
        connection_options: connection_options,
        reconnection_backoff_ms: reconnection_backoff_ms
      ]

      {Worker, init_args}
    end
  end
end
