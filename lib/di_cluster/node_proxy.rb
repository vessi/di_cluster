require 'msgpack'
require 'socket'

module DiCluster
  class NodeProxy
    attr_reader :node_id

    def initialize(role, node_id, options = {})
      @address = options['address']
      @node_id = node_id
      @namespace = role
      @proxy_actor = ProxyActor.spawn name: :proxy_actor, args: [{namespace: @namespace, address: @address}]
    end

    def method_missing(method_name, *args)
      @proxy_actor.ask! Message.new(procedure: method_name, arguments: args)
    end
  end

  class ProxyActor < Concurrent::Actor::RestartingContext
    def initialize(options = {})
      @namespace = options[:namespace]
      @address = options[:address]
      uri = URI.parse(@address)
      host = uri.host
      port = uri.port
      @socket = TCPSocket.new(host, port)
    end

    def on_message message
      full_method_name = "#{@namespace}.#{message.procedure}"
      @socket.puts ({procedure: full_method_name, arguments: message.arguments}).to_msgpack
      response = @socket.gets
      return MessagePack.unpack(response.chomp)
    end
  end
end
