# frozen_string_literal: true

require 'concurrent'
require 'concurrent-edge'

require 'di_cluster/version'
require 'di_cluster/node'
require 'di_cluster/node_proxy'

# Di(stributed) Cluster
module DiCluster
  module ClassMethods
    def start(options={})
      @started = true
      @registry = options[:registry]
      @nodes_cache = Set.new
      @node = Node.new(options)
    end

    def cluster
      self
    end

    def me
      @node
    end

    def with_role(role_name)
      # Add balancer support
      nodes_list = @registry.nodes_with_role(role_name)
      selected_node_id = nodes_list.keys.shuffle.sample
      selected_node = nodes_list[selected_node_id]
      puts 'replying from node id ' + selected_node_id
      proxy = @nodes_cache.find { |n| n[:role] == role_name && n[:proxy].node_id == selected_node_id }
      if proxy.nil?
        proxy = {role: role_name, proxy: NodeProxy.new(role_name, selected_node_id, selected_node)}
        @nodes_cache << proxy
      end
      proxy[:proxy]
    end

    def environment
      @environment
    end

    def ttl_rate
      @ttl_rate
    end

    def logger
      @logger
    end

    def logger=(logger)
      @logger = logger
    end

    def running?
      @started
    end

    def environment=(value)
      raise ReadOnlyProperty if @started
      @environment = value
    end

    def ttl_rate=(value)
      raise ReadOnlyProperty if @started
      @ttl_rate = value
    end
  end

  extend ClassMethods
  @environment = ENV['DI_CLUSTER_ENV'] || 'development'
  @ttl_rate = 5
  @started = false
  @logger = Logger.new(STDOUT)
end
