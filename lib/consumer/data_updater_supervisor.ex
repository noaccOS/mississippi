defmodule Mississippi.Consumer.DataUpdater.Supervisor do
  require Logger
  use DynamicSupervisor

  def start_link(init_args) do
    DynamicSupervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  @impl true
  def init(init_args) do
    _ = Logger.info("Starting DataUpdater supervisor", tag: "data_updater_supervisor_start")
    DynamicSupervisor.init(strategy: :one_for_one, extra_arguments: init_args)
  end

  def start_child(child) do
    DynamicSupervisor.start_child(__MODULE__, child)
  end

  def terminate_child(pid) do
    _ =
      Logger.info("Terminating a DataUpdater",
        tag: "data_updater_terminate"
      )

    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end
end
