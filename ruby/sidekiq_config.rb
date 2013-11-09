Sidekiq.configure_server do |config|
  config.redis = { :url => 'redis://ifp2:6379', :namespace => 'isucon' }
end

Sidekiq.configure_client do |config|
  config.redis = { :url => 'redis://ifp2:6379:6379', :namespace => 'isucon' }
end