# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.AMQPDataConsumer.Starter do
  @moduledoc false
  use Task, restart: :transient

  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  def start_link(args) do
    Task.start_link(__MODULE__, :start_consumers, [args])
  end

  def start_consumers(args) do
    start_consumers(args, 10)
  end

  defp start_consumers(_, 0) do
    _ = Logger.warning("Cannot start AMQPDataConsumers")
    {:error, :cannot_start_consumers}
  end

  defp start_consumers(args, retry) do
    AMQPDataConsumer.Supervisor.start_children(args)

    queue_total = args[:total_count]

    child_count =
      AMQPDataConsumer.Supervisor |> DynamicSupervisor.which_children() |> Enum.count()

    case child_count do
      ^queue_total ->
        :ok

      _ ->
        # TODO: do we want something more refined, e.g. exponential backoff?
        Process.sleep(:timer.seconds(2))
        start_consumers(args, retry - 1)
    end
  end
end
