require_relative '../../config/keys.local.rb'
require 'csv'
require 'uri'
require 'net/http'
require 'json'
require 'pg'
require 'openssl'

DEBUG=false
PGPWD=DEBUG ? PGPWD_TEST : PGPWD_LIVE
PGNAME=DEBUG ? PGNAME_TEST : PGNAME_LIVE
PGUSER=DEBUG ? PGUSER_TEST : PGUSER_LIVE
PGHOST=DEBUG ? PGHOST_TEST : PGHOST_LIVE
WEBHOOK=DEBUG ? WEBHOOK_TEST : WEBHOOK_LIVE

ctx,cmd=ARGV[0].split(':') if ARGV[0]
value=ARGV[1]
if ctx.nil? or cmd.nil? then
	puts <<END
LIVE ? #{!DEBUG}
* search:user <lastname>
* search:candidate <name>
* reallow:search
* reallow:user_id <id>
* reset:user_id <id>
* grantaccess:search <lastname>
* grantaccess:nb <nb>
* grantaccess:user_id <id>
* blockaddcandidate:search
* blockaddcandidate:user_id <id>
* blockcandidatereview:user_id <id>
* banuser:user_id <id>
* unblock:user_id <id>
* betacodes:gen <nb>
* betacodes:search
* broadcast:all <input_file>
* broadcast:user_id <user_id> <input_file>
END
	exit
end

def send_command(data)
	uri = URI.parse(WEBHOOK)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Post.new("/#{PREFIX}/command")
	request.add_field("Secret-Key",SECRET)
	request.add_field('Content-Type', 'application/json')
	request.body = JSON.dump(data)
	http.request(request)
end

def generate_code(size = 6)
	charset = %w{ 2 3 4 6 7 9 A C D E F G H J K M N P Q R T V W X Y Z}
	(0...size).map{ charset.to_a[rand(charset.size)] }.join
end

data2=<<END
{
	"update_id": -1,
	"message": {
		"message_id": 0,
		"from": {
			"id": "%{user_id}",
			"first_name": "%{firstname}",
			"last_name": "%{lastname}",
			"username": "%{username}"
		},
		"chat": {
			"id": "%{user_id}",
			"first_name": "%{firstname}",
			"last_name": "%{lastname}",
			"username": "%{username}",
			"type": "private"
		},
		"date": %{date},
		"text": %{cmd}
	}
}
END

db=PG.connect(
	"dbname"=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)

case ctx
when 'search'
	case cmd
	when 'user'
		search_waiting_list="SELECT user_id,firstname,lastname,username,email,registered FROM citizens WHERE lastname ILIKE $1 ORDER BY registered ASC"
		res=db.exec_params(search_waiting_list,[value])
		if not res.num_tuples.zero? then
			res.each do |r|
				puts "#{r['user_id']} (#{r['registered']}) #{r['firstname']} #{r['lastname']} (@#{r['username']} #{r['email']})"
			end
		end
	when 'candidate'
		search_waiting_list="SELECT candidate_id,name,gender,trello,photo,date_added FROM candidates WHERE name ILIKE $1 ORDER BY date_added ASC"
		res=db.exec_params(search_waiting_list,[value])
		if not res.num_tuples.zero? then
			res.each do |r|
				puts "(#{r['date_added']}) #{r['name']} #{r['gender']} (#{r['trello']}) : #{r['candidate_id']}"
			end
		end
	end
when 'reallow'
	case cmd
	when 'search'
		search_not_allowed_users=<<END
SELECT user_id,firstname,lastname,registered,settings::json#>'{blocked,not_allowed}' AS not_allowed
FROM citizens 
WHERE settings::json#>>'{blocked,not_allowed}'='true'
ORDER BY registered ASC
END
		res=db.exec_params(search_not_allowed_users,[])
		if not res.num_tuples.zero? then
			res.each do |r|
				puts "(#{r['registered']}) #{r['firstname']} #{r['lastname']} : #{r['user_id']}"
			end
		end
	when 'user_id'
		get_user_from_waiting_list=<<END
SELECT user_id,firstname,lastname,username
FROM citizens
WHERE user_id=$1
ORDER BY registered ASC
END
		res=db.exec_params(get_user_from_waiting_list, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/allow_user".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'grantaccess'
	case cmd
	when 'search'
		if value then
			search_waiting_list="SELECT user_id,firstname,lastname,registered FROM waiting_list WHERE lastname ILIKE $1 ORDER BY registered ASC"
			res=db.exec_params(search_waiting_list,[value])
		else
			search_waiting_list="SELECT user_id,firstname,lastname,registered FROM waiting_list ORDER BY registered ASC"
			res=db.exec(search_waiting_list)
		end
		if not res.num_tuples.zero? then
			res.each_with_index do |r,i|
				i+=1
				puts "#{i} (#{r['registered']}) #{r['firstname']} #{r['lastname']} : #{r['user_id']}"
			end
		end
	when 'nb'
		read_waiting_list="SELECT user_id,firstname,lastname,registered FROM waiting_list ORDER BY registered ASC LIMIT $1"
		res=db.exec_params(read_waiting_list,[value])
		if not res.num_tuples.zero? then
			res.each do |r|
				send_command(JSON.parse(data2 % {
					cmd:"api/access_granted".to_json,
					user_id:r['user_id'],
					firstname:r['firstname'],
					lastname:r['lastname'],
					username:r['username'],
					date:Time.now().to_i
				}))
				puts "Access granted to user #{r['user_id']} : #{r['firstname']} #{r['lastname']} registered on #{r['registered']}"
				sleep(1.0/4.0)
			end
		end
	when 'user_id'
		get_user_from_waiting_list="SELECT user_id FROM waiting_list WHERE user_id=$1 ORDER BY registered ASC"
		res=db.exec_params(get_user_from_waiting_list, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/access_granted".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'blockaddcandidate'
	case cmd
	when 'search'
		search_biggest_candidates_proposers="SELECT user_id,firstname,lastname,registered,settings::json#>>'{actions,nb_candidates_proposed}' AS nb_proposed FROM citizens ORDER BY nb_proposed DESC LIMIT 50"
		res=db.exec(search_biggest_candidates_proposers)
		if not res.num_tuples.zero? then
			res.each do |r|
				puts "[#{r['registered']}] #{r['firstname']} #{r['lastname']} (#{r['user_id']}) : #{r['nb_proposed']}"
			end
		end
	when 'user_id'
		get_user_from_waiting_list="SELECT user_id FROM citizens WHERE user_id=$1"
		res=db.exec_params(get_user_from_waiting_list, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/block_candidate_proposals".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'banuser'
	case cmd
	when 'user_id'
		get_user="SELECT user_id FROM citizens WHERE user_id=$1"
		res=db.exec_params(get_user, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/ban_user".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'unblock'
	case cmd
	when 'user_id'
		get_user="SELECT user_id FROM citizens WHERE user_id=$1"
		res=db.exec_params(get_user, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/unblock_user".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'blockcandidatereview'
	case cmd
	when 'user_id'
		get_user="SELECT user_id FROM citizens WHERE user_id=$1"
		res=db.exec_params(get_user, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/block_candidate_reviews".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'betacodes'
	case cmd
	when 'search'
		get_codes="SELECT * FROM beta_codes"
		res=db.exec(get_codes)
		if not res.num_tuples.zero? then
			res.each do |r|
				puts "#{r['code']}"
			end
		end
	when 'gen'
		codes=[]
		query=[]
		idx=1
		value.to_i.times do
			codes.push(generate_code())
			query.push("($#{idx})")
			idx+=1
		end
		query_str=query.join(',')+" RETURNING *"
		insert_codes="INSERT INTO beta_codes (code) VALUES "+query_str
		res=db.exec_params(insert_codes, codes)
		if not res.num_tuples.zero? then
			res.each do |r|
				puts "#{r['code']}"
			end
		end
	end
when 'reset'
	case cmd
	when 'user_id'
		get_user="SELECT user_id FROM citizens WHERE user_id=$1"
		res=db.exec_params(get_user, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd:"api/reset_user".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	end
when 'broadcast'
	case cmd
	when 'user_id'
		input_file=ARGV[2]
		exit("missing input file") if input_file.nil? or !File.exist?(input_file)
		msg=File.read(input_file).strip
		get_user="SELECT user_id FROM citizens WHERE user_id=$1"
		res=db.exec_params(get_user, [value])
		if not res.num_tuples.zero? then
			send_command(JSON.parse(data2 % {
				cmd: "api/broadcast\n#{msg.strip}".to_json,
				user_id:res[0]['user_id'],
				firstname:res[0]['firstname'],
				lastname:res[0]['lastname'],
				username:res[0]['username'],
				date:Time.now().to_i
			}))
		end
	when 'all'
		input_file=ARGV[1]
		exit("missing input file") if input_file.nil? or !File.exist?(input_file)
		msg=File.read(input_file).strip
		get_users="SELECT user_id,firstname,lastname FROM citizens"
		res=db.exec(get_users)
		if not res.num_tuples.zero? then
			res.each do |r|
				send_command(JSON.parse(data2 % {
					cmd: "api/broadcast\n#{msg.strip}".to_json,
					user_id:r['user_id'],
					firstname:r['firstname'],
					lastname:r['lastname'],
					username:r['username'],
					date:Time.now().to_i
				}))
				puts "Broadcast msg sent to #{r['user_id']} : #{r['firstname']} #{r['lastname']}"
				sleep(1.0/3.0)
			end
		end
	end
end
