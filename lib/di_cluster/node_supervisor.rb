module DiCluster
  class NodeSupervisor < Concurrent::Actor::RestartingContext

    def initialize(options = {})
      @name = options[:name]
      @registry = options[:registry]
      @address = options[:address]
      @roles_actors = Set.new
    end

    def on_message(message)
      case message.procedure.to_sym
      when :register_node
        register_node
      when :register_role
        register_role(*(message.arguments))
      when :roles
        roles
      when :exec
        mth = message.arguments
        role, meth = mth.procedure.split('.')
        actor = @roles_actors.find { |r| r[:name] == role }[:actor]
        actor.ask! Message.new(procedure: meth, arguments: mth.arguments)
      end
    end

    def register_node
      @registry.register(@name, {roles: roles, ttl: (Time.now.to_i + DiCluster.ttl_rate), address: @address})
    end

    def register_role(role_name, role_actor, arguments)
      @roles_actors << {
        name: role_name,
        actor: role_actor.spawn(name: role_name, args: arguments)
      }
      register_node
    end

    def roles
      @roles_actors.map { |actor| actor[:name] }
    end

  end
end
