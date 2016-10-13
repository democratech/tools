require_relative '../config/keys.local.rb'
require 'csv'
require 'uri'
require 'net/http'
require 'json'
require 'pg'
require 'openssl'
require 'digest/md5'

DEBUG=false
PGPWD=DEBUG ? PGPWD_TEST : PGPWD_LIVE
PGNAME=DEBUG ? PGNAME_TEST : PGNAME_LIVE
PGUSER=DEBUG ? PGUSER_TEST : PGUSER_LIVE
PGHOST=DEBUG ? PGHOST_TEST : PGHOST_LIVE
WEBHOOK=DEBUG ? WEBHOOK_TEST : WEBHOOK_LIVE

db=PG.connect(
	"dbname"=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)

def get_mailchimp_groups()
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Get.new("/3.0/lists/"+MCLIST+"/interest-categories/"+MCCANDIDATESGROUPS+"/interests?count=100&offset=0")
	request.basic_auth 'hello',MCKEY
	res=http.request(request)
	return JSON.parse(res.body)["interests"]
end

def update_member_tags(mailchimp_id,groups)
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Patch.new("/3.0/lists/"+MCLIST+"/members/"+mailchimp_id)
	request.basic_auth 'hello',MCKEY
	request.add_field('Content-Type', 'application/json')
	request.body = JSON.dump({
		'interests'=>groups
	})
	res=http.request(request)
	return res.kind_of? Net::HTTPSuccess
end


def update_member(id,member) 
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Put.new("/3.0/lists/"+MCLIST+"/members/"+id)
	request.basic_auth 'hello',MCKEY
	request.add_field('Content-Type', 'application/json')
	request.body = member
	return http.request(request)
end

def check_batch_status(batch_id)
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Get.new("/3.0/batches/#{batch_id}")
	request.basic_auth 'hello',MCKEY
	res=http.request(request)
	return JSON.parse(res.body)["status"]
end


def send_batch(batch)
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Post.new("/3.0/batches")
	request.basic_auth 'hello',MCKEY
	request.add_field('Content-Type', 'application/json')
	request.body = JSON.dump(batch)
	res=http.request(request)
	puts "send_batch ans: #{res.body}"
	return JSON.parse(res.body)["id"]
end

def get_members()
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Get.new("/3.0/lists/"+MCLIST+"/members")
	request.basic_auth 'hello',MCKEY
	return http.request(request)
end

#1 On construit les groupes d'interet present dans la bdd
puts "Retrieving supporters from db..."
users="select f.email, array_agg(c.mc_group_id) as groups from followers as f inner join candidates as c on (c.candidate_id=f.candidate_id and c.qualified) group by f.email"
res=db.exec(users)
db_users={}
res.each do |u|
	db_users[Digest::MD5.hexdigest(u['email'])]=u['groups'][1..-2].split(',')
end

#puts "OK (#{db_groups.length} tags found)"

#2 On créé l'operation batch de mise à jour des supporteurs sur mailchimp
puts "creating batch operations"
batch={ "operations"=>[] }
db_users.each do |k,v|
	cmd={
		"method"=>"PATCH",
		"path"=>"lists/#{MCLIST}/members/#{k}",
		"operation_id"=>k,
		"body"=>{"interests"=>{}}
	}
	v.each {|g| cmd["body"]["interests"][g]=true }
	cmd["body"]=JSON.dump(cmd["body"])
	batch["operations"].push(cmd)
end
#puts JSON.pretty_generate(batch)
puts "batch is ready"

#3 executing the batch operation
batch_id=send_batch(batch)
status="pending"
while status!="finished" do
	sleep(2)
	print "checking batch status..."
	status=check_batch_status(batch_id)
	puts status
end
