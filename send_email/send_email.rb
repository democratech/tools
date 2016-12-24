require '../config/keys.local.rb'
require 'mandrill'
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

mandrill=Mandrill::API.new(MANDRILLKEY)
db=PG.connect(
	:dbname=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)

def email_candidat_trello(db,mandrill)
	message= {  
		:from_name=> "LaPrimaire.org",  
		:subject=> "Préparez-vous : Ouverture de LaPrimaire.org le 4 avril prochain !",  
		:to=>[  
			{  
				:email=> "thib@thib.fr",
				:name=> "Thibauld"
			}  
		],
		:merge_vars=>[
			{
				:rcpt=>"Jacques",
				:vars=>[
					{
						:name=>"UUID",
						:content=>"john"
					},
					{
						:name=>"BETACODE",
						:content=>"doe"
					},
				]
			}
		]
	}
	get_candidates="SELECT candidate_id,name,email FROM candidates WHERE email IS NOT NULL"
	res_candidats=db.exec(get_candidates)
	if not res_candidats.num_tuples.zero? then
		nb_codes=res_candidats.num_tuples
		codes=[]
		query=[]
		idx=1
		nb_codes.to_i.times do
			codes.push(generate_code())
			query.push("($#{idx})")
			idx+=1
		end
		query_str=query.join(',')+" RETURNING *"
		insert_codes="INSERT INTO beta_codes (code) VALUES "+query_str
		res_codes=db.exec_params(insert_codes,codes)
		return "error adding codes" if res_codes.num_tuples.zero? 
		emails={}
		res_candidats.each_with_index do |r,i|
			emails[r['email']]={"UUID"=>r['candidate_id'],"BETACODE"=>codes[i],"NAME"=>r['name']}
		end
	end
	emails.each do |k,v|
		begin
			msg=message
			msg[:to][0][:email]=k
			msg[:to][0][:name]=v["NAME"]
			msg[:merge_vars][0][:rcpt]=k
			msg[:merge_vars][0][:vars][0][:content]=v["UUID"]
			msg[:merge_vars][0][:vars][1][:content]=v["BETACODE"]
			result=mandrill.messages.send_template("laprimaire-org-candidates-part-i-trello",[],message)
			puts "sending email to #{v['NAME']} (#{k}) with UUID #{v['UUID']} and CODE #{v['BETACODE']}"
			sleep(1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_candidat_formation(db,mandrill)
	get_candidates="SELECT candidate_id,name,email FROM candidates WHERE email IS NOT NULL AND verified"
	res_candidats=db.exec(get_candidates)
	if not res_candidats.num_tuples.zero? then
		emails=[]
		res_candidats.each do |r|
			message= {
				:from_name=> "LaPrimaire.org",  
				:subject=> "Candidats à LaPrimaire.org : réservez votre samedi 21 mai prochain !",  
				:to=>[  {  :email=> "#{r['email']}" }  ]
			}
			emails.push(message)
		end
	end
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-candidates-part-ii-formation",[],k)
			puts "sending email to #{k[:to][0][:email]}"
			sleep(0.2)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_candidat_admin(db,mandrill)
	get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified"
	res_candidats=db.exec(get_candidates)
	if not res_candidats.num_tuples.zero? then
		emails=[]
		res_candidats.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}",
					:name=> "#{r['name']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ {:name=>"CANDIDATE_KEY",:content=>"#{r['candidate_key']}"} ]
				}]
			}
			emails.push(message)
		end
	end
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-candidates-part-iii-admin",[],k)
			puts result.inspect
			puts "sending email to #{k[:to][0][:email]}"
			sleep(0.1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_donateurs_defisc(db,mandrill)
	require 'date'
	message= {
		:to=>[
			{
				:email=> "thib@thib.fr",
				:name=> "Thibauld"
			}  
		],
		:merge_vars=>[
			{
				:vars=>[
					{
						:name=>"DON",
						:content=>"10"
					},
					{
						:name=>"DATE_DON",
						:content=>"12.03.2015"
					},
					{
						:name=>"PRENOM",
						:content=>"Jacques"
					},
					{
						:name=>"MOYEN",
						:content=>"paypal"
					},
				]
			}
		]
	}
	donateurs_csv=CSV.read(ARGV[0])
	donateurs=[] #donateurs
	donateurs_csv.each do |d|
		next if d[0]=='id'
		donateurs.push({
			:id=>d[0],
			:from=>d[1],
			:date=>d[3],
			:amount=>d[4],
			:firstname=>d[6],
			:lastname=>d[7],
			:email=>d[8]
		})
	end
	donateurs.each do |k|
		begin
			from= k[:from]=='stripe' ? 'carte bleue' : k[:from]
			msg=message
			msg[:to][0][:email]=k[:email]
			msg[:to][0][:name]="#{k[:firstname]} #{k[:lastname]}"
			msg[:merge_vars][0][:rcpt]=k[:email]
			msg[:merge_vars][0][:vars][0][:content]=k[:amount]
			msg[:merge_vars][0][:vars][1][:content]=Date.parse(k[:date]).strftime("%d/%m/%Y")
			msg[:merge_vars][0][:vars][2][:content]=k[:firstname]
			msg[:merge_vars][0][:vars][3][:content]=from
			result=mandrill.messages.send_template("laprimaire-org-email-aux-donateurs-i",[],message)
			puts "sending email to #{k[:firstname]} #{k[:lastname]} (#{k[:email]}) pour un don de #{k[:amount]} le #{k[:date]} via #{from}"
			sleep(0.1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_donateurs_defisc_update(db,mandrill)
	donateurs_csv=CSV.read(ARGV[0])
	emails=[] #donateurs
	donateurs_csv.each do |d|
		next if d[0]=='id'
		message= {
			:to=>[{
				:email=> "#{d[8]}"
			}]
		}
		emails.push(message)
	end
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-email-aux-donateurs-ii-bad-news",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			sleep(0.1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_candidat_programme(db,mandrill)
	get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified"
	#get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND email='tfavre@gmail.com'"
	res_candidats=db.exec(get_candidates)
	if not res_candidats.num_tuples.zero? then
		emails=[]
		res_candidats.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}",
					:name=> "#{r['name']}"
				}]
			}
			emails.push(message)
		end
	end
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-candidates-iv-programme",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			sleep(0.1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_candidat_urgent(db,mandrill)
	#get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND email='tfavre@gmail.com'"
	get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified AND vision is NULL"
	res_candidats=db.exec(get_candidates)
	if not res_candidats.num_tuples.zero? then
		emails=[]
		res_candidats.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}",
					:name=> "#{r['name']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ {:name=>"CANDIDATE_KEY",:content=>"#{r['candidate_key']}"} ]
				}]
			}
			emails.push(message)
		end
	end
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-candidates-part-v-urgent",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			sleep(0.1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
end

def email_citoyens_appel_aux_maires(db,mandrill)
	#get_citizens="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	#get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified AND vision is NULL"
	get_citizens="select c.email from citizens as c where c.email NOT IN (select ci.email from citizens as ci inner join mongo_supporteurs as su on (su.email=ci.email));"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ 
						{:name=>"EMAIL",:content=>"#{r['email']}"},
						{:name=>"LNAME",:content=>"#{r['lastname']}"},
						{:name=>"FNAME",:content=>"#{r['firstame']}"}
					]
				}]
			}
			emails.push(message)
		end
	end
	results=[]
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-citoyens-appel-aux-maires-ii",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1/30)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20160601_email_shoot.txt",JSON.dump(results))
end

def email_citoyens_toutes_candidates(db,mandrill)
	#get_citizens="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	#get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified AND vision is NULL"
	#get_citizens="select c.email from citizens as c where c.email NOT IN (select ci.email from citizens as ci inner join mongo_supporteurs as su on (su.email=ci.email));"
	get_citizens="select u.email, u.firstname, u.lastname from users as u where zipcode IN ('75000','75001','75002','75003','75004','75005','75006','75007','75008','75009','75010','75011','75012','75013','75014','75015','75016','75017','75018','75019','75020') and (email_status=0 or email_status=2)"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ 
						{:name=>"EMAIL",:content=>"#{r['email']}"},
						{:name=>"LNAME",:content=>"#{r['lastname']}"},
						{:name=>"FNAME",:content=>"#{r['firstame']}"}
					]
				}]
			}
			emails.push(message)
		end
	end
	results=[]
	emails.each do |k|
		begin

			result=mandrill.messages.send_template("laprimaire-org-appel-aux-benevoles-en-idf",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1/30)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20160609_email_shoot.txt",JSON.dump(results))
end

def email_recap_soutiens(db,mandrill)
	#get_citizens="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	#get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified AND vision is NULL"
	#get_citizens="select c.email from citizens as c where c.email NOT IN (select ci.email from citizens as ci inner join mongo_supporteurs as su on (su.email=ci.email));"
	get_citizens="select u.email, u.firstname, u.lastname from users as u where telegram_id is not null;"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ 
						{:name=>"EMAIL",:content=>"#{r['email']}"},
						{:name=>"LNAME",:content=>"#{r['lastname']}"},
						{:name=>"FNAME",:content=>"#{r['firstame']}"}
					]
				}]
			}
			emails.push(message)
		end
	end
	results=[]
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("transactional-recap-soutiens",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1/30)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20160609_email_shoot.txt",JSON.dump(results))
end

def email_citizens_no_access(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	get_citizens="select email,firstname,lastname,user_key from users as u where tags @> ARRAY['voter'] AND email_status=2 AND email not in (select distinct email from auth_history)"
	#get_candidates="SELECT candidate_id,candidate_key,name,email FROM candidates WHERE email IS NOT NULL AND verified AND vision is NULL"
	#get_citizens="select c.email from citizens as c where c.email NOT IN (select ci.email from citizens as ci inner join mongo_supporteurs as su on (su.email=ci.email));"
	#get_citizens="select u.email, u.firstname, u.lastname from users as u where telegram_id is not null;"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ 
						{:name=>"EMAIL",:content=>"#{r['email']}"},
						{:name=>"LNAME",:content=>"#{r['lastname']}"},
						{:name=>"USER_KEY",:content=>"#{r['user_key']}"},
						{:name=>"FNAME",:content=>"#{r['firstame']}"}
					]
				}]
			}
			emails.push(message)
		end
	end
	results=[]
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-citoyens-no-access",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1/4)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20161106_email_shoot_citizens_no_access.txt",JSON.dump(results))
end

def email_citizens_vote_issue(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	get_citizens="select distinct u.email,u.user_key from ballots as b inner join candidates_ballots as cb on (cb.ballot_id=b.ballot_id) inner join users as u on (u.email=b.email) where cb.vote_status is not null AND cb.vote_status!='complete' AND cb.date_notified<'2016-11-06' AND cb.date_notified>'2016-10-26' order by u.email asc"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
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
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-erreur-when-voting",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1/4)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20161106_email_shoot_citizens_vote_issue.txt",JSON.dump(results))
end

def email_citizens_missing_votes(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname, 3 as missing FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	get_citizens="select y.email,y.missing,u.user_key from (select z.email,5-array_length(z.votes,1) as missing from (select b.email,array_agg(cb.vote_status) as votes from candidates_ballots as cb inner join ballots as b on (b.ballot_id=cb.ballot_id) where cb.vote_status='complete' and cb.date_notified<'2016-11-06' group by b.email) as z) as y inner join users as u on (u.email=y.email) where y.missing>0 and u.email_status=2 order by y.email asc"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
				:to=>[{
					:email=> "#{r['email']}"
				}],
				:merge_vars=>[{
					:rcpt=>"#{r['email']}",
					:vars=>[ 
						{:name=>"EMAIL",:content=>"#{r['email']}"},
						{:name=>"NB_CANDIDATS",:content=>"#{r['missing']}"},
						{:name=>"USER_KEY",:content=>"#{r['user_key']}"}
					]
				}]
			}
			emails.push(message)
		end
	end
	results=[]
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-citoyens-x-votes-missing",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1/4)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20161106_email_shoot_citizens_missing_votes.txt",JSON.dump(results))
end

def email_citizens_ballot_without_vote(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname, 3 as missing FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	get_citizens="select z.email,u.user_key from (select b.email,array_agg(cb.vote_status) FILTER (WHERE vote_status is not null) as votes from candidates_ballots as cb inner join ballots as b on (b.ballot_id=cb.ballot_id) group by b.email) as z inner join users as u on (u.email=z.email) where z.votes is null and u.email_status=2 order by z.email asc"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
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
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-ballot-without-vote",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(0.8)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20161106_email_shoot_citizens_ballot_wo_vote.txt",JSON.dump(results))
end

def email_citizens_pre_authenticate(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname, 3 as missing FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	#get_citizens="select email,user_key from users where validation_level<3 and email_status=2 and (registered>'2016-11-07' and registered<'2016-12-12') order by registered asc"
	get_citizens="select email,user_key from users where validation_level<3 and email_status=2 and (registered<'2016-11-07' and registered>'2016-07-01') order by registered asc"
	res_citizens=db.exec(get_citizens)
	if not res_citizens.num_tuples.zero? then
		emails=[]
		res_citizens.each do |r|
			message= {
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
	emails.each do |k|
		begin
			result=mandrill.messages.send_template("laprimaire-org-pre-auth-2nd-tour-de-vote",[],k)
			puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
			results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			sleep(1)
		rescue Mandrill::Error => e
			msg="A mandrill error occurred: #{e.class} - #{e.message}"
			puts msg
		end
	end
	File.write("20161212_email_shoot_citizens_pre_auth_2.txt",JSON.dump(results))
end

def email_citizens_authenticated_2nd_vote(db,mandrill)
	#get_citizens="SELECT user_key,email,firstname,lastname, 3 as missing FROM users WHERE email IS NOT NULL AND email='tfavre@gmail.com' LIMIT 2"
	#get_citizens="select email,user_key from users where validation_level<3 and email_status=2 and (registered>'2016-11-07' and registered<'2016-12-12') order by registered asc"
	#get_citizens="select u.email,u.user_key from users as u where validation_level<3 and email_status=2 and registered<'2016-03-01'" #DONE
	#get_citizens="select u.email,u.user_key from users as u where validation_level<3 and email_status=2 and (registered>'2016-03-01' and registered<'2016-04-01')" # 1500/2000 DONE
	get_citizens="select u.email,u.user_key from users as u where validation_level<3 and email_status=2 and (registered>'2016-04-01' and registered<'2016-11-01')"
	#get_citizens="select u.email,u.user_key from users as u where validation_level<3 and email_status=2 and (registered>'2016-04-01' and registered<'2016-05-01')"
	subject="Pour un VRAI choix en 2017 : plus que quelques jours pour voter !"
	#get_citizens="select u.email,u.user_key from users as u where validation_level<3 and email_status=2 and (registered>'2016-11-01' and registered<'2016-12-15')"
	#get_citizens="select u.email,u.user_key from users as u where validation_level<3 and email_status=2 and (registered>'2016-07-01' and registered<'2016-11-01')"
	#subject="Votez et changez la donne en 2017 !"
	#get_citizens="select email,user_key from tmp_emails order by email asc"
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
				result=mandrill.messages.send_template("laprimaire-org-2eme-tour-de-vote",[],k)
				puts "sending email to #{k[:to][0][:email]} #{result.inspect}"
				results.push({:email=>result[0]['email'], :status=>result[0]['status'],:reject_reason=>result[0]['reject_reason'],:id=>result[0]['_id']})
			rescue Mandrill::Error => e
				msg="A mandrill error occurred: #{e.class} - #{e.message}"
				puts msg
			end
		#end
		sleep(0.5)
	end
	#threads.each {|t| t.join}
	File.write("20161223_email_shoot_not_authenticated_2nd_vote#2.txt",JSON.dump(results))
end

email_citizens_authenticated_2nd_vote(db,mandrill)
#email_citizens_pre_authenticate(db,mandrill) #DONE
#email_citizens_missing_votes(db,mandrill) #DONE
#email_citizens_ballot_without_vote(db,mandrill) #DONE
#email_citizens_vote_issue(db,mandrill)
#email_citizens_no_access(db,mandrill)
#email_citoyens_toutes_candidates(db,mandrill)
#email_citoyens_appel_aux_maires(db,mandrill)
#email_candidat_urgent(db,mandrill)
#email_candidat_programme(db,mandrill)
#email_donateurs_defisc_update(db,mandrill)
#email_donateurs_defisc(db,mandrill)
#email_candidat_admin(db,mandrill)
#email_candidat_formation(db,mandrill)
