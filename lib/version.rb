module Bonnie
  class Version
    def self.current()
      return APP_CONFIG["version"]
    end
  end
end
