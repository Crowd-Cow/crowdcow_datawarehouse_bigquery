{% docs setup_dev_env %}

This macro to sets up a development environment for a user by cloning a given database.
Based on the given action, it will create or destroy a development database based on the user 
running the macro and what target environment they are using. 

For example:
  - User `user1` is using the default dbt target of `dev` and clones the proddb database
    - The database that is created or destroyed: user_db = `user1_proddb_dev`


  - User `user2` is using the target flag of --target qa and clones the proddb database
    - The database that is created or destroyed: user_db = `user2_proddb_qa` 
    
Using the `dry_run` action will output the sql statements that would run given the create or destroy action
This can be useful to make sure the clone/drop table statement will do what is intended 

Command to run the macro from the command line: `dbt run-operation setup_dev_env --args '{ "clone_db": "<db_name>", "action":"<create/destroy/dry_run>"}'`

{% enddocs %}