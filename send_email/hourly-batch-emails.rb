require '../config/keys.local.rb'
require 'mandrill'
require 'pg'
require 'csv'

DEBUG=false
TEST_EMAIL=false
PGPWD=DEBUG ? PGPWD_TEST : PGPWD_LIVE
PGNAME=DEBUG ? PGNAME_TEST : PGNAME_LIVE
PGUSER=DEBUG ? PGUSER_TEST : PGUSER_LIVE
PGHOST=DEBUG ? PGHOST_TEST : PGHOST_LIVE


def generate_code(size = 6)
	charset = %w{ 2 3 4 6 7 9 A C D E F G H J K M N P Q R T V W X Y Z}
	(0...size).map{ charset.to_a[rand(charset.size)] }.join
end

mandrill=Mandrill::API.new(MANDRILLKEY)
db=PG.connect(
	:dbname=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)

if TEST_EMAIL then
	puts "TEST_EMAIL"
end

def tag_voters(db,mandrill)
	if TEST_EMAIL then
		users=[{'email'=>'tfavre@gmail.com','user_key'=>'XXXX'}]
	else
		q="UPDATE users as u SET tags=array_append(tags,'voteur2') FROM ballots as b WHERE b.email=u.email AND NOT (u.tags @> ARRAY['voteur2']) AND b.vote_id=1 AND b.vote_status='success' RETURNING u.email, u.user_key"
		res=db.exec(q)
		users=res.num_tuples.zero? ? nil : res
	end
	send_emails(mandrill,users,'laprimaire-org-vote-merci-avec-don',"Votre vote a bien été enregistré !") unless users.nil?
	return users
end

def tag_donors(db,mandrill)
	if TEST_EMAIL then
		users=[{'email'=>'tfavre@gmail.com','user_key'=>'XXXX','amount'=>100.00}]
	else
		q="UPDATE users as u SET tags=array_append(tags,'donateur_parti') FROM donations as d WHERE d.email=u.email AND NOT (u.tags @> ARRAY['donateur_parti']) AND d.origin='payzen' RETURNING u.email, u.user_key,d.amount"
		res=db.exec(q)
		users=res.num_tuples.zero? ? nil : res
	end
	send_emails(mandrill,users,"laprimaire-org-don-thank-you","Merci énormément pour votre don !") unless users.nil?
	return users
end

def send_emails(mandrill,users,template,subject)
	return if (users.nil? || template.nil? || subject.nil?)
	emails=[]
	users.each do |r|
		message= {
			:subject=>subject,
			:to=>[{
				:email=> "#{r['email']}"
			}],
			:merge_vars=>[{
				:rcpt=>"#{r['email']}",
				:vars=>[ 
					{:name=>"EMAIL",:content=>"#{r['email']}"},
					{:name=>"USER_KEY",:content=>"#{r['user_key']}"},
					{:name=>"AMOUNT",:content=>"#{r['amount']}"}
				]
			}]
		}
		emails.push(message)
	end
	results=[]
	emails.each do |k|
		begin
			result=mandrill.messages.send_template(template,[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write(Time.new.strftime("%Y-%m%d-%H:%M")+template+".txt",JSON.dump(results))
end

tag_voters(db,mandrill)
tag_donors(db,mandrill)
