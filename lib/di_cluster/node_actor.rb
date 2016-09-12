module DiCluster
  class NodeActor < Concurrent::Actor::RestartingContext
    def on_message(message)
      tell :terminate! and return if message.procedure.to_sym == :stop
      self.send(message.procedure.to_sym, *message.arguments)
    end
  end
end
