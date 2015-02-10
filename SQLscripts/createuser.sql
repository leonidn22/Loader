-- SELECT DISABLE_LOCAL_SEGMENTS();

create user dbuser identified by 'Passw0rd';
alter user dbuser SEARCH_PATH public;

grant usage on schema public to dbuser;
grant usage on schema data to dbuser;
--grant select on data.CDR to dbuser;
grant select on public.CDR to dbuser;

grant ALL on public.CDR_AGG to dbuser;


sudo useradd -r dbuser -c 'HP Vertica database user'

sudo passwd dbuser

sudo chmod 01777 /opt/allot/vftrk
