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
	request = Net::HTTP::Get.new("/3.0/lists/"+MCLIST+"/interest-categories/"+MCGROUPCAT+"/interests?count=100&offset=0")
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

def create_interest(interest_name)
	uri = URI.parse(MCURL)
	http = Net::HTTP.new(uri.host, uri.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE
	request = Net::HTTP::Post.new("/3.0/lists/"+MCLIST+"/interest-categories/"+MCGROUPCAT+"/interests")
	request.basic_auth 'hello',MCKEY
	request.add_field('Content-Type', 'application/json')
	request.body = JSON.dump({ 'name'=>interest_name })
	res=http.request(request)
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

#1 On recupere les groupes d'interets mailchimp
print "Retrieving mailchimp interest groups..."
groups=get_mailchimp_groups()
mc_groups=[]
if not groups.nil? then
	groups.each do |g|
		mc_groups.push(g['id'].downcase)
	end
end
puts "OK (#{mc_groups.length} groups found)"

#2 On construit les groupes d'interet present dans la bdd
print "Retrieving users from db..."
users="SELECT * FROM users ORDER BY registered ASC"
res=db.exec(users)
db_groups=[]
db_users={}
res.each do |u|
	db_users[Digest::MD5.hexdigest(u['email']]=u['email']
	if not u['tags'].nil?
		tags=u['tags'][1..-2].gsub("\"","").split(",")
		tags.each do |t|
			db_groups.push(t) unless db_groups.include?(t)
		end
	end
end
puts "OK (#{db_groups.length} tags found)"

#3 On determine quels sont les nouveaux groupes d'interets a creer sur mailchimp
to_create=db_groups-mc_groups
if not to_create.empty? then
	to_create.each do |ng|
		mc_groups.push(create_interest(ng))
	end
	puts "Creating missing interest groups on mailchimp...OK"
else
	puts "Mailchimp interests groups match users tags...OK"
end

# On recupere la liste des inscrits sur MC
print "Retrieving subscribed users from mailchimp..."
res.each do |u|
	md5=Digest::MD5.hexdigest(u['email'])
	tags=u['tags']
	user_groups={}
	member={}
	groups.each do |i|
		if (tags.include? i["name"].downcase) then
			user_groups[i["id"]]=true
		else
			user_groups[i["id"]]=false
		end
	end
	puts "#{u['email']} : #{md5} / tags #{u['tags']} / groups #{mc_groups}"
end

exit
res=get_members()
if not res.nil? then
	members = JSON.parse(res.body)
	members['members'].each do |m|
		info=m['merge_fields']
		if info['CITY'].empty? and not info['ZIPCODE'].empty? then
			info['ZIPCODE']=info['ZIPCODE'].delete(' ')
			if not info['ZIPCODE'].match('^[0-9]{5}(?:-[0-9]{4})?$').nil? and not zipcodes[info['ZIPCODE']].nil? then
				m['merge_fields']['CITY']=zipcodes[info['ZIPCODE']][0]
				m['merge_fields']['ZIPCODE']=info['ZIPCODE']
				res=update_member(m['id'],JSON.dump(m))
			end
		end
	end
end
