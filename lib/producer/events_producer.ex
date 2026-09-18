# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Producer.EventsProducer do
  @moduledoc """
  The entry point for publishing messages on Mississippi.

  Publish is now sharded: it hashes the `sharding_key` and routes the
  message to the per-queue `Worker` that owns the corresponding queue.
  """

  alias AMQP.Basic
  alias Mississippi.Producer.EventsProducer.Options
  alias Mississippi.Producer.EventsProducer.Worker

  # API

  @doc """
  Publish a message on Mississippi AMQP queues. The call is blocking, as only one message at a time can be published on a given shard.
  """
  @type publish_opts() :: keyword()
  @type mississippi_config() :: keyword()
  @spec publish(
          payload :: binary(),
          publish_opts :: publish_opts(),
          mississippi_config :: mississippi_config()
        ) ::
          :ok | {:error, :reconnecting} | Basic.error()
  def publish(payload, publish_opts, mississippi_config) do
    publish_opts = NimbleOptions.validate!(publish_opts, Options.publish_opts())
    sharding_key = publish_opts[:sharding_key]

    queue_count = get_in(mississippi_config, [:queues, :total_count])

    valid_config? = is_integer(queue_count) and queue_count > 0
    invalid_config? = not valid_config?

    if invalid_config? do
      raise NimbleOptions.ValidationError,
        key: :total_count,
        keys_path: [:queues, :total_count],
        value: queue_count,
        message: "expected :total_count to be a positive integer"
    end

    Worker.for_sharding_key(sharding_key, queue_count)
    |> Worker.publish(payload, publish_opts)
  end
end
