DO $$
BEGIN

  RAISE NOTICE 'WHO DAT? %', user;
  RAISE NOTICE 'Just me, %', user;
  RAISE NOTICE 'Guess we are talking to ourselves!  %', user;

END $$;