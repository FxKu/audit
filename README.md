Audit
=====

Audit is a versioning approach for PostgreSQL using PL/pgSQL functions


0. Index
--------

1. License
2. About
3. System requirements
4. How To
5. Future Plans
6. Developers
7. Contact
8. Special thanks
9. Disclaimer


1. License
----------

The skripts for Audit are open source under GNU Lesser General 
Public License Version 3.0. See the file LICENSE for more details. 


2. About
--------

The auditing approach of Audit is nothing new. Define triggers to log
changes on your database is a well known practice. I thought if I know
everything that happened in my database in the past I might be able to 
go back step by step and take a look at my database at a certain point
of time. That was the first idea for audit.

At pgConf.DE 2013 I asked Hans-Jürgen Schönig what he thinks about this 
idea and he gave me a very good advice to use the JSON functionalties 
of PostgreSQL to have a very generic auditing routine.

As I managed to recreate my database from my log table I thought the user
should also be able to recreate an old database state as to be the actual
one again. Sort of a rollback to any state when the database
has been changed in the past.

For further reading please consider that Audit has not been tested or
or benchmarked a lot by myself. The processes described above are working
to my satisfaction but maybe you expect more or something different. See
also section 5.



3. System requirements
----------------------

* PostgreSQL 9.3 


4. How To
-----------------

As said the logging works with a trigger that inserts values into a log
table (audit_log). If a row is inserted or updated the new values are
stored in the audit_log table by using the function row_to_JSON(record).
If a row has been deleted or the table has been truncated all the event
informations are logged except for the row values as they are already 
stored in the audit_log table after being inserted or updated.

When creating a table state for a given date the audit_log table is 
queried for all dates (timestamps) the requested table had been changed 
before the requested point of time (sorted in descending order). For 
each timestamp the audit_log table is queried for inserted or updated 
rows. An additional audit_id column in the original table helps to 
identify rows that appear at multiple dates e.g. if certain rows have 
been updated several times. Only the newest entry of each row will end 
up in the target table.

OK, let's start:

4.1. Contents of Audit

Audit is defined as a separate schema containing the log table, the 
trigger procedure and the PL/pgSQL functions which are mostly executed
on a single table or a whole schema.

4.2. Start Audit

After creating the Audit schema the user is free to combine the 
functions to match his workflows. One possibilty can be to execute the
procedure create_schema_audit('name_of_schema',ARRAY['not_this_table',
'not_that_table',etc.]).

Log triggers and audit_id columns will then be created for all tables
in the given schema except for those tables that were excluded in the
second function parameter. The procedure does also commit a first
insert in the audit_log table declaring all existing rows in a table
as 'inserted' rows. This will be the initial state for the temporal
versioning. The user will not be able to recreate his database before
this date - not with Audit at least.

When setting up a new database I would recommend to use this procedure
after data has been imported. Otherwise several different timestamps 
might appear in the audit_log table. The more timestamps are recorded 
the more queries are necessary to produce a table state.

4.3. Produce a past state of your database

A table state is produced with the procedure 'produce_table_state'. A 
whole database state might be produced with 'produce_schema_state'.
The result is written to another schema defined by the user. It can 
defined as a VIEW (default) or a TABLE.

4.4. Work with the past state

producing tables happens without defining primary keys or indexes.
references between tables are lost as well. If the user wants to work
on the produced table or database state - like he would do with the
recent state - he can use the procedures 'pkey_table_state', 
'fkey_table_state' and 'index_table_state'. These procedures create
primary keys, foreign keys and indexes on behalf of recent constraints
defined in the certain schema (e.g. 'public'). If table and/or database
structures have changed fundamentally over time it might not be possible
to recreate constraints and indexes. 

4.5. Declare the past state as the actual state

This fact is also very important for the next step. If a produced 
database state should be declared as the actual state the function 
'recreate_schema_state' can be used. BUT BE CAREFUL! It truncates and
drops the recent state completely, copies the old state and recreates 
constraints and indexes on behalf of the old state. If step 4.4. has
been missed out the new recent state will be left without
any primary keys, foreign keys or indexes.


5. Future Plans
--------------------------------------

First of all I want to to share my idea with the PostgreSQL community
and discuss the skripts in order to improve them. Let me know what you
think of it. I know, it might not be the best solution for write-
extensive databases.

I would be very happy if there are other PostgreSQL developers out there
who are interested in Audit and willing to help me to improve it.

Together we might create a powerful, easy-to-use versioning approach 
for PostgreSQL. I would love to see it becoming an extension as well 
but yet I have no idea how this works.

If there are similar solutions like Audit, please let me know. I'm not
fixed on this project. If there is a better approach out there I would
use it, of course :)

  
6. Developers
-------------

Felix Kunde <fkunde@virtualcitysystems.de>


7. Contact
----------

fkunde@virtualcitysystems.de


8. Special Thanks
-----------------

Hans-Jürgen Schönig (Cybertech) 
  --> recommend to use a generic JSON auditing
Claus Nagel (virtualcitySYSTEMS) 
  --> conceptual advices about logging
Ollyc (Stackoverflow) 
  --> Query to list all foreign keys of a table
Denis de Bernardy (Stackoverflow, mesoconcepts) 
  --> Query to list all indexes of a table


9. Disclaimer
--------------

AUDIT IS PROVIDED "AS IS" AND "WITH ALL FAULTS." 
I MAKE NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE 
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR 
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL I BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL, 
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF 
I HAVE BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
STILL, I WOULD FEEL SORRY.