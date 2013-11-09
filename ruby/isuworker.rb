require 'sidekiq'
require './sidekiq_config'


# Run Sidekiq with
# bundle exec sidekiq -C ./sidekiq.yml -r ./isuworker.rb
class Isuworker
  include Sidekiq::Worker

  def perform(args={})
    #Do Stuff with worker
    1.upto(100) do |index|
      puts index
    end
  end
end