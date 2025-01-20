defmodule NodeListener do
  @moduledoc false
  use GenServer

  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def init(args) do
    :net_kernel.monitor_nodes(true, node_type: :visible)

    processes = Keyword.get(args, :processes, [])
    queues_config = Keyword.get(args, :queues_config, [])
    state = %{processes: processes, queues_config: queues_config}

    {:ok, state}
  end

  def handle_info({:nodeup, node, node_type}, state) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is up")
    %{processes: processes, queues_config: queues_config} = state

    set_members(processes)
    # _ = AMQPDataConsumer.Starter.start_consumers(queues_config)

    {:noreply, queues_config}
  end

  def handle_info({:nodedown, node, node_type}, state) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is down")
    %{processes: processes, queues_config: queues_config} = state

    set_members(processes)
    # _ = AMQPDataConsumer.Starter.start_consumers(queues_config)

    {:noreply, queues_config}
  end

  defp set_members(processes) do
    members = [Node.self() | Node.list()]

    for name <- processes do
      Logger.info("setting members for #{inspect(name)}")
      named_members = Enum.map(members, &{name, &1})
      :ok = Horde.Cluster.set_members(name, named_members)
    end

    :ok
  end
end
