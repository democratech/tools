require '../config/keys.local.rb'
require 'twilio-ruby'
require 'pg'
require 'csv'

DEBUG=false
PGPWD=DEBUG ? PGPWD_TEST : PGPWD_LIVE
PGNAME=DEBUG ? PGNAME_TEST : PGNAME_LIVE
PGUSER=DEBUG ? PGUSER_TEST : PGUSER_LIVE
PGHOST=DEBUG ? PGHOST_TEST : PGHOST_LIVE


def generate_code(size = 6)
	charset = %w{ 2 3 4 6 7 9 A C D E F G H J K M N P Q R T V W X Y Z}
	(0...size).map{ charset.to_a[rand(charset.size)] }.join
end
client=Twilio::REST::Client.new(TWILIO_ACC_SID,TWILIO_AUTH_TOKEN);
db=PG.connect(
	:dbname=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)
tels={
	'US'=>'+18607852796',
	'CA'=>'+12044005886',
	'FR'=>'+33 6 44 60 55 61'
}
get_citizens="select t.country_code,u.email,u.user_key,u.telephone from users as u inner join telephones as t on (t.international=u.telephone) where validation_level=3 and not tags @> ARRAY['voteur2'] and telephone is not null and t.is_cellphone and u.registered>'2016-01-01'"
res_citizens=db.exec(get_citizens)
if not res_citizens.num_tuples.zero? then
	res_citizens.each do |r|
		begin
			from=tels['FR']
			from=tels['US'] if r['country_code']=='US'
			from=tels['CA'] if r['country_code']=='CA'
			client.messages.create(
				from: from,
				to: r['telephone'],
				body: "Bonjour, n'oubliez pas de voter sur LaPrimaire.org : https://laprimaire.org/citoyen/vote/#{r['user_key']}/2 Fin du vote demain !"
			);
			puts "Sent SMS to #{r['email']} : tel #{r['telephone']} / key #{r['user_key']}"
			sleep(0.3)
		rescue Twilio::REST::RequestError => e
			puts "A Twilio error occurred: #{e.class} - #{e.message}"
		end
	end
end
exit
def email_citizens_account_validated(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname, 3 as missing FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	get_citizens="select u.email,u.user_key from users as u where tags @> ARRAY['manual_validation']"
	subject="Votre compte vient d'être validé, vous pouvez désormais voter !"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
				:subject=>subject,
				:to=>[{
					:email=> "#{r['email']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ 
						{:name=>"EMAIL",:content=>"#{r['email']}"},
						{:name=>"USER_KEY",:content=>"#{r['user_key']}"}
					]
				}]
			}
			emails.push(message)
		end
	end
	results=[]
	threads=[]
	emails.each do |k|
		#threads << Thread.new do
			begin
				result=mandrill.messages.send_template("laprimaire-org-compte-valid-manuellement",[],k)
				puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
				results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			rescue Mandrill::Error => e
				msg="A mandrill error occurred: #{e.class} - #{e.message}"
				puts msg
			end
		#end
		sleep(1)
	end
	#threads.each {|t| t.join}
	File.write("20161228_email_shoot_account_validated.txt",JSON.dump(results))
end

email_citizens_account_validated(db,mandrill)
