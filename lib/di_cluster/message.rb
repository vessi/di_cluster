module DiCluster
  class Message
    attr_reader :procedure, :arguments
    def initialize(options = {})
      @procedure = options[:procedure]
      @arguments = options[:arguments] || []
    end
  end
end
