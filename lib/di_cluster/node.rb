require 'json'
require 'securerandom'
require 'di_cluster/message'
require 'di_cluster/node_actor'
require 'di_cluster/node_supervisor'

module DiCluster
  class Node
    def initialize(options)
      @name = options[:name] || generate_name
      @registry = options[:registry]
      @address = options[:address] || 'tcp://0.0.0.0:5555'
      @supervisor = NodeSupervisor.spawn name: @name, args: [{
        registry: @registry,
        name: @name,
        address: @address}]
      register_node
      register_role('heartbeat', HeartbeatActor, [{name: @name, registry: @registry, ttl_rate: DiCluster.ttl_rate}])
      register_role('rpc', RPCActor, [{address: @address}])
    end

    def stop
      @supervisor.tell :stop
    end

    def register_role role_name, role_actor, args
      @supervisor.tell Message.new(procedure: :register_role, arguments: [role_name, role_actor, args])
    end

    def register_node
      @supervisor.tell Message.new(procedure: :register_node, arguments: [])
    end

    def roles
      @supervisor.ask! Message.new(procedure: :roles)
    end

    private
    def generate_name
      SecureRandom.uuid
    end
  end

  class RPCActor < Concurrent::Actor::RestartingContext
    def initialize(options = {})
      @listener = Listener.spawn name: :listener, supervise: true, args: [{address: options[:address]}]
    end

    def on_message(message)
      case message.procedure
      when :stop
        @listener << :terminate!
        @executor << :terminate!
        tell :terminate!
      when :exec
        parent.ask!(message)
      end
    end
  end

  class Listener < Concurrent::Actor::RestartingContext
    def initialize(options = {})
      uri = URI.parse(options[:address])
      @server = TCPServer.new(uri.host, uri.port)
      @started = true
      while @started
        Thread.start(@server.accept) do |client|
          while encoded = client.gets
            decoded = MessagePack.unpack(encoded.chomp)
            mth = Message.new(procedure: decoded['procedure'], arguments: decoded['arguments'])
            result = parent.ask! Message.new(procedure: :exec, arguments: mth)
            client.puts result.to_msgpack
          end
        end
      end
    end

    def on_message(message)
      case message.procedure.to_sym
      when :stop then @started = false
      end
    end
  end

  class HeartbeatActor < Concurrent::Actor::Context
    def initialize(options)
      @registry = options[:registry]
      @node = options[:name]
      @ttl = options[:ttl_rate]
      @task = Concurrent::TimerTask.new(execution_interval: @ttl, timeout_interval: @ttl*2 - 1, run_now: true) do
        parent.tell Message.new(procedure: :register_node)
      end
      @task.execute
    end

    def on_message(msg)
      case msg.procedure.to_sym
      when :stop then @task.shutdown
      end
    end
  end
end
