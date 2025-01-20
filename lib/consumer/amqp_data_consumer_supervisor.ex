# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.Supervisor do
  @moduledoc false
  use Horde.DynamicSupervisor

  alias Horde.DynamicSupervisor
  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg,
      name: __MODULE__,
      distribution_strategy: Horde.UniformQuorumDistribution
    )
  end

  @impl true
  def init(init_arg) do
    with {:ok, result} <- DynamicSupervisor.init(strategy: :one_for_one, process_redistribution: :active) do
      init_arg
      |> Keyword.get(:queues_config, [])
      |> start_children()

            AMQPDataConsumer.Supervisor |> DynamicSupervisor.which_children() |> Enum.count()
            |> dbg()


      {:ok, result}
    end
    
  end

  defp start_children(queues_config) do
    children = amqp_data_consumers_childspecs(queues_config)

    Enum.each(children, fn child ->
      DynamicSupervisor.start_child(Mississippi.Consumer.AMQPDataConsumer.Supervisor, child)
    end)
  end

  defp amqp_data_consumers_childspecs(queues_config) do
    dbg(queues_config)
    queue_total = queues_config[:total_count]
    queue_prefix = queues_config[:prefix]

    for queue_index <- 0..(queue_total - 1) do
      queue_name = "#{queue_prefix}#{queue_index}"

      init_args = [
        queue_name: queue_name,
        queue_index: queue_index
      ]

      Supervisor.child_spec({AMQPDataConsumer, init_args}, id: {AMQPDataConsumer, queue_index})
    end
  end
end
