require 'sidekiq'
require './sidekiq_config'
require_relative 'app' unless defined? Isucon3Final

# Run Sidekiq with
# bundle exec sidekiq -C ./sidekiq.yml -r ./isuworker.rb
class Isuworker
  include Sidekiq::Worker
  include Isucon3Final::Isucon3FinalHelper

  def perform(args={})

  	if args["icon"]
  		create_icons args["icon"]
  	elsif args["image"]
  		create_images args["image"]
  	end
  end
end