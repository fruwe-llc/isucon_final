require 'sinatra/base'
require 'sinatra/json'
require 'json'
require 'mysql2-cs-bind'
require 'digest/sha2'
require 'tempfile'
require 'fileutils'
require 'uuid'
require 'redis'

class Isucon3Final < Sinatra::Base
  $stdout.sync = true

  unless defined? ICON_S
    TIMEOUT  = 30
    INTERVAL =  2
    $UUID    = UUID.new

    ICON_S  =  32
    ICON_M  =  64
    ICON_L  = 128
    IMAGE_S = 128
    IMAGE_M = 256
    IMAGE_L = nil
  end

  module Isucon3FinalHelper
    def load_config
      return $config if $config
      $config = JSON.parse(IO.read(File.dirname(__FILE__) + "/../config/#{ ENV['ISUCON_ENV'] || 'local' }.json"))
    end

    def redis
      @redis ||= Redis.new("host" => "localhost", "port" => 6379, :db => 5, :driver => :hiredis)
    end

    def connection
      config = load_config['database']
      return $mysql if $mysql
      $mysql = Mysql2::Client.new(
        :host      => config['host'],
        :port      => config['port'],
        :username  => config['username'],
        :password  => config['password'],
        :database  => config['dbname'],
        :reconnect => true,
      )
    end

    def convert(orig, ext, w, h, perform_caching=true)
      redis_key = "convert:#{orig}:#{ext}:#{w}:#{h}"

      data = redis.get redis_key if perform_caching
      return data if data

      data = nil

      Tempfile.open('isucontemp') do |tmp|
        newfile = "#{tmp.path}.#{ext}"
        `convert -geometry #{w}x#{h} #{orig} #{newfile}`
        File.open(newfile, 'r+b') do |new|
          data = new.read
        end
        File.unlink(newfile)
      end

      redis.set redis_key, data if perform_caching

      data
    end

    def crop_square(orig, ext)
      identity = `identify #{orig}`
      (w, h)   = identity.split[2].split('x').map(&:to_i)

      if w > h
        pixels = h
        crop_x = ((w - pixels) / 2).floor
        crop_y = 0
      elsif w < h
        pixels = w
        crop_x = 0
        crop_y = ((h - pixels) / 2).floor
      else
        pixels = w
        crop_x = 0
        crop_y = 0
      end

      tmp     = Tempfile.open("isucon")
      newfile = "#{tmp.path}.#{ext}"
      `convert -crop #{pixels}x#{pixels}+#{crop_x}+#{crop_y} #{orig} #{newfile}`
      tmp.close
      tmp.unlink

      newfile
    end

    def get_user
      api_key = env["HTTP_X_API_KEY"] || request.cookies["api_key"]

      user = db_user_by_api_key api_key
    end

    def require_user(user)
      unless user
        halt 400, "400 Bad Request"
      end
    end

    def uri_for(path)
      scheme = request.scheme
      if (scheme == 'http' && request.port == 80 ||
          scheme == 'https' && request.port == 443)
        port = ""
      else
        port = ":#{request.port}"
      end
      base = "#{scheme}://#{request.host}#{port}#{request.script_name}"
      "#{base}#{path}"
    end

    def params_with_multi_value(key)
      value = Rack::Utils.parse_query(@env['rack.request.form_vars'])[key]
      value.is_a?(Array) ? value : [value]
    end

    def next_uuid
      Digest::SHA256.hexdigest($UUID.generate)
    end

    def create_icons icon
      [ICON_S, ICON_M, ICON_L].each do |image_size|
        create_icon icon, image_size
      end
    end

    def create_icon icon, image_size
      dir = load_config['data_dir']
      icon_path = "#{dir}/icon/#{icon}.png"
      convert(icon_path, 'png', image_size, image_size, true)
    end

    def create_images image
      [IMAGE_S, IMAGE_M, IMAGE_L].each do |image_size|
        create_image image, image_size
      end
    end

    # accepts IMAGE_S, IMAGE_M, IMAGE_L(nil)
    def create_image image, image_size
      dir = load_config['data_dir']
      redis_key = "crop_square:#{image}:#{image_size.to_i}"

      data = redis.get redis_key

      if data
        # nop
      elsif image_size
        file = crop_square("#{dir}/image/#{image}.jpg", 'jpg')
        data = convert(file, 'jpg', image_size, image_size, false)
        File.unlink(file)

        redis.set redis_key, data
      else
        file = File.open("#{dir}/image/#{image}.jpg", 'r+b')
        data = file.read
        file.close

        redis.set redis_key, data
      end

      return data
    end

    # database accesses

    def db_users
      mysql   = connection
      users = mysql.xquery('SELECT * FROM users').entries
    end

    def db_entries
      db_users.collect do |user|
        db_user_entries_by_last_entry user["id"]
      end.flatten.compact
    end

    def db_user_by_api_key api_key
      return nil unless api_key

      mysql   = connection
      user = mysql.xquery('SELECT * FROM users WHERE api_key = ?', api_key).first

      return user
    end

    def db_user_by_id user_id
      return nil unless user_id

      mysql   = connection
      user = mysql.xquery('SELECT * FROM users WHERE id = ?', user_id).first

      return user
    end

    def db_add_user name, api_key
      mysql = connection

      mysql.xquery(
        'INSERT INTO users (name, api_key, icon) VALUES (?, ?, ?)',
        name, api_key, 'default'
      )

      user = db_user_by_id mysql.last_id

      return user
    end

    def db_set_user_icon user_id, icon
      mysql = connection

      mysql.xquery(
        'UPDATE users SET icon = ? WHERE id = ?',
        icon, user_id
      )

      return nil
    end

    def db_create_entry user_id, image_id, publish_level
      mysql = connection

      mysql.xquery(
        'INSERT INTO entries (user, image, publish_level, created_at) VALUES (?, ?, ?, NOW())',
        user_id, image_id, publish_level
      )

      id    = mysql.last_id
      # entry = mysql.xquery('SELECT * FROM entries WHERE id = ?', id).first

      return id
    end

    def db_entry_by_id entry_id
      mysql = connection

      entry = mysql.xquery('SELECT * FROM entries WHERE id = ?', entry_id).first

      return entry
    end

    def db_entry_delete_by_id entry_id
      mysql = connection

      mysql.xquery('DELETE FROM entries WHERE id = ?', entry_id)

      return nil
    end

    def db_entry_by_image image
      mysql = connection

      entry = mysql.xquery('SELECT * FROM entries WHERE image = ?', image).first

      return entry
    end

    def db_is_follow user_id, target_id
      mysql = connection

      follow = mysql.xquery(
        'SELECT * FROM follow_map WHERE user = ? AND target = ?',
        user_id, target_id
      ).first

      is_following = !!follow

      return is_following
    end

    def db_users_followed_by_user_id user_id
      mysql = connection
      
      following = mysql.xquery(
        'SELECT users.* FROM follow_map JOIN users ON (follow_map.target = users.id) WHERE follow_map.user = ? ORDER BY follow_map.created_at DESC',
        user_id
      )

      return following
    end

    def db_insert_to_user_followers user_id, targets
      mysql = connection

      targets.each do |target|
        mysql.xquery(
          'INSERT IGNORE INTO follow_map (user, target, created_at) VALUE (?, ?, NOW())',
          user_id, target
        )
      end

      return nil
    end

    def db_delete_from_user_followers user_id, targets
      mysql = connection
      
      targets.each do |target|
        mysql.xquery(
          'DELETE FROM follow_map WHERE user = ? AND target = ?',
          user_id, target
        )
      end

      return nil
    end

    def db_user_entries_by_last_entry user_id, latest_entry = nil
      mysql = connection

      if latest_entry
          sql = 'SELECT * FROM (SELECT * FROM entries WHERE (user=? OR publish_level=2 OR (publish_level=1 AND user IN (SELECT target FROM follow_map WHERE user=?))) AND id > ? ORDER BY id LIMIT 30) AS e ORDER BY e.id DESC'
          params = [user_id, user_id, latest_entry]
      else
          sql = 'SELECT * FROM entries WHERE (user=? OR publish_level=2 OR (publish_level=1 AND user IN (SELECT target FROM follow_map WHERE user=?))) ORDER BY id DESC LIMIT 30'
          params = [user_id, user_id]
      end

      entries = mysql.xquery(sql, *params)

      return entries
    end
  end

  helpers Isucon3FinalHelper

  get '/' do
    File.read(File.join('public', 'index.html'))
  end

  post '/signup' do
    name = params[:name]
    unless name.match(/\A[0-9a-zA-Z_]{2,16}\z/)
      halt 400, "400 Bad Request"
    end

    api_key = next_uuid

    user = db_add_user name, api_key

    json({
      :id      => user["id"].to_i,
      :name    => user["name"],
      :icon    => uri_for("/icon/#{ user["icon"] }"),
      :api_key => user["api_key"]
    })
  end

  get '/me' do
    user = get_user
    require_user(user)

    json({
      :id      => user["id"].to_i,
      :name    => user["name"],
      :icon    => uri_for("/icon/#{ user["icon"] }")
    })
  end

  get '/icon/:icon' do
    icon = params[:icon]
    size = params[:size] || 's'
    dir  = load_config['data_dir']

    icon_path = "#{dir}/icon/#{icon}.png"

    w = size == 's' ? ICON_S
      : size == 'm' ? ICON_M
      : size == 'l' ? ICON_L
      :               ICON_S
    h = w

    redis_key = "convert:#{icon_path}:png:#{w}:#{w}"
    data = redis.get redis_key

    unless data
      halt 404
    end

    content_type 'image/png'
    data
  end

  post '/icon' do
    user  = get_user
    require_user(user)

    upload = params[:image]
    unless upload
      halt 400, "400 Bad Request"
    end
    unless upload[:type].match(/^image\/(jpe?g|png)$/)
      halt 400, "400 Bad Request"
    end

    file = crop_square(upload[:tempfile].path, 'png')
    icon = next_uuid
    dir  = load_config['data_dir']
    FileUtils.move(file, "#{dir}/icon/#{icon}.png") or halt 500

    db_set_user_icon user["id"], icon

    create_icons icon

    json({
      :icon => uri_for("/icon/#{icon}")
    })
  end

  post '/entry' do
    user  = get_user
    require_user(user)

    upload = params[:image]
    unless upload
      halt 400, "400 Bad Request"
    end
    unless upload[:type].match(/^image\/jpe?g$/)
      halt 400, "400 Bad Request"
    end

    image_id = next_uuid
    dir      = load_config['data_dir']
    FileUtils.move(upload[:tempfile].path, "#{dir}/image/#{image_id}.jpg") or halt 500

    publish_level = params[:publish_level]

    entry_id = db_create_entry user["id"], image_id, publish_level

    create_images image_id

    json({
      :id            => entry_id,
      :image         => uri_for("/image/#{image_id}"),
      :publish_level => publish_level.to_i,
      :user => {
        :id   => user["id"].to_i,
        :name => user["name"],
        :icon => uri_for("/icon/#{user["icon"]}")
      }
    })
  end

  post '/entry/:id' do
    user  = get_user
    require_user(user)

    id  = params[:id].to_i

    entry = db_entry_by_id id

    halt 404 unless entry
    halt 400, "400 Bad Request" unless entry["user"] == user["id"] && params["__method"] == 'DELETE'

    db_entry_delete_by_id id

    json({
      :ok => true
    })
  end

  get '/image/:image' do
    user  = get_user

    image = params[:image]
    size  = params[:size] || 'l'

    entry = db_entry_by_image image

    unless entry
      halt 404
    end
    if entry["publish_level"] == 0
      if user && entry["user"] == user["id"]
        # publish_level==0 はentryの所有者しか見えない
        # ok
      else
        halt 404
      end
    elsif entry["publish_level"] == 1
      # publish_level==1 はentryの所有者かfollowerしか見えない
      if user && entry["user"] == user["id"]
        # ok
      elsif user
        follow = db_is_follow user["id"], entry["user"]

        halt 404 unless follow
      else
        halt 404
      end
    end

    w = size == 's' ? IMAGE_S
      : size == 'm' ? IMAGE_M
      : size == 'l' ? IMAGE_L
      :               IMAGE_L
    h = w

    redis_key = "crop_square:#{image}:#{w.to_i}"
    data = redis.get redis_key

    # raise "Chris f**ked up - run preparation script" unless data
    data = create_image image, w unless data

    content_type 'image/jpeg'
    data
  end

  def get_following
    user  = get_user
    require_user(user)

    following = db_users_followed_by_user_id user["id"]

    headers "Cache-Control" => "no-cache"
    json({
      :users => following.map do |u|
        {
          :id   => u["id"].to_i,
          :name => u["name"],
          :icon => uri_for("/icon/#{u["icon"]}")
        }
      end
    })
  end

  get '/follow' do
    get_following
  end

  post '/follow' do
    user  = get_user
    require_user(user)

    targets = params_with_multi_value('target') - [user["id"]]

    db_insert_to_user_followers user["id"], targets

    get_following
  end

  post '/unfollow' do
    user  = get_user
    require_user(user)

    targets = params_with_multi_value('target') - [user["id"]]

    db_delete_from_user_followers user["id"], targets

    get_following
  end

  get '/timeline' do
    user  = get_user
    require_user(user)

    latest_entry = params[:latest_entry]

    start        = Time.now.to_i
    entries      = []

    while Time.now.to_i - start < TIMEOUT
      _entries = db_user_entries_by_last_entry user["id"], latest_entry

      if _entries.size == 0
        sleep INTERVAL
        next
      else
        entries      = _entries
        latest_entry = entries.first["id"]
        break
      end
    end

    headers "Cache-Control" => "no-cache"
    json({
      :latest_entry => latest_entry.to_i,
      :entries => entries.map do |entry|

        user = db_user_by_id entry["user"]

        {
          :id            => entry["id"].to_i,
          :image         => uri_for("/image/#{entry["image"]}"),
          :publish_level => entry["publish_level"].to_i,
          :user => {
            :id   => user["id"].to_i,
            :name => user["name"],
            :icon => uri_for("/icon/#{user["icon"]}")
          }
        }
      end
    })
  end

  get '/preload' do
    require './isuworker'

    redis.flushdb

    users = db_users
    users.each do |user|
      Isuworker.perform_async({:icon => user["icon"]})
    end

    entries = db_entries
    entries.each do |entry|
      Isuworker.perform_async({:image => entry["image"]})
    end

    halt 200, "ok"
  end

  run! if app_file == $0
end
