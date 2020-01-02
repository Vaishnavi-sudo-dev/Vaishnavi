import csv
import psycopg2
conn = psycopg2.connect(user="postgres", password="Vai123&*", database="random", host="localhost", port=5432)
cur=conn.cursor()
truncate_lz="truncate table loading_zone"
cur.execute(truncate_lz)
with open('Day2.txt','r') as csv_file:
    csv_reader=csv.reader(csv_file)
    next(csv_reader)
    for line in csv_reader:
        try:
            insert_loading_zone_query = (f"insert into public.loading_zone(aid, a_name, addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber, attendancekey, attendancedate, attendedyesno) "
            f"values( {line[0]}, '{line[1]}', {line[2]}, '{line[3]}', '{line[4]}', '{line[5]}', '{line[6]}', '{line[7]}', {line[8]}, {line[9]}, {line[10]}, '{line[11]}', '{line[12]}')")
            cur.execute(insert_loading_zone_query)
            conn.commit()
        except Exception as e:
            print("Error:", str(e))
            conn.rollback()
    try:
        trunc_stage_table = "truncate table stg_address"
        cur.execute(trunc_stage_table)
        conn.commit()
    except Exception as e:
         print("Error:", str(e))
         conn.rollback()
    try:
        insert_stage_table = "INSERT INTO stg_address (id_key, aid,addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber) SELECT MD5(CONCAT(aid::text,addrid::text)),aid, addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber from loading_zone"
        cur.execute(insert_stage_table)
        conn.commit()
        view_st = "create view a as (select s.aid from stg_address as s inner join address on s.aid=address.aid)"
        cur.execute(view_st)
        inorup_stage_table = "update stg_address set action_indicator = case when aid in ( select aid from a) then 'U' else 'I' end"
        cur.execute(inorup_stage_table)
        cur.execute("drop view a")
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        print("check if already updated")
        conn.rollback()
    try:
        insert_address_table = "INSERT INTO address (id_key,aid, addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber) SELECT  id_key,aid,addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber from stg_address"
        cur.execute(insert_address_table)
        conn.commit()
        upn_base_table = "update address set active_ind='N' where aid in ( select aid from stg_address where action_indicator='U')"
        cur.execute(upn_base_table)
        view_base = " create view d as (select s.id_key,row_number() over (partition by s.aid order by s.addrid) from stg_address as s inner join address on s.aid=address.aid)"
        cur.execute(view_base)
        upy_base_table = "update address set active_ind='Y' where id_key in ( select id_key from stg_address where id_key in (select id_key from d where row_number=1))"
        cur.execute(upy_base_table)
        cur.execute("drop view d")
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        print("check if already updated")
        conn.rollback()
    try:
        trunc_stage_table = "truncate table stg_student"
        cur.execute(trunc_stage_table)
        conn.commit()
    except Exception as e:
         print("Error:", str(e))
         conn.rollback()
    try:
        insert_stage_table = "INSERT INTO stg_student (id_key,aid,a_name,attendancekey) SELECT MD5(CONCAT(aid::text,a_name::text)), aid, a_name,attendancekey from loading_zone"
        cur.execute(insert_stage_table)
        conn.commit()
        view_st = "create view b as (select s.aid from stg_student as s inner join student on s.aid=student.aid)"
        cur.execute(view_st)
        inorup_stage_table = "update stg_student set action_indicator = case when aid in ( select aid from b) then 'U' else 'I' end"
        cur.execute(inorup_stage_table)
        cur.execute("drop view b")
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        print("check if already updated")
        conn.rollback()
    try:
        insert_student_table = "INSERT INTO student (id_key,aid, a_name,attendancekey) SELECT id_key,aid, a_name,attendancekey from stg_student"
        cur.execute(insert_student_table)
        conn.commit()
        upn_base_table = "update student set active_ind='N'"
        cur.execute(upn_base_table)
        view_base1 = " create view c as (select s.id_key,row_number() over (partition by s.aid order by s.attendancekey) from stg_student as s inner join student on s.aid=student.aid)"
        cur.execute(view_base1)
        upy_base_table = "update student set active_ind='Y' where id_key in ( select id_key from stg_student where id_key in (select id_key from c where row_number=1))"
        cur.execute(upy_base_table)
        cur.execute("drop view c")
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        print("check if already updated")
        conn.rollback()
    try:
        trunc_stage_table = "truncate table stg_attendance"
        cur.execute(trunc_stage_table)
        conn.commit()
    except Exception as e:
         print("Error:", str(e))
         conn.rollback()
    try:
        insert_stage_table = "INSERT INTO stg_attendance (id_key,aid,attendancekey,attendancedate,attendedyesno) SELECT MD5(CONCAT(attendancekey::text,attendancedate::text)),aid,attendancekey,attendancedate,attendedyesno from loading_zone"
        cur.execute(insert_stage_table)
        conn.commit()
        view_st = "create view g as (select s.aid from stg_attendance as s inner join attendance on s.aid=attendance.aid)"
        cur.execute(view_st)
        inorup_stage_table = "update stg_attendance set action_indicator = case when aid in ( select aid from g) then 'U' else 'I' end"
        cur.execute(inorup_stage_table)
        cur.execute("drop view g")
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        print("check if already updated")
        conn.rollback()
    try:
        insert_attendance_table = "INSERT INTO attendance (id_key,aid,attendancekey,attendancedate,attendedyesno) SELECT id_key,aid,attendancekey,attendancedate,attendedyesno from stg_attendance"
        cur.execute(insert_attendance_table)
        conn.commit()
        upn_base_table = "update attendance set active_ind='N'"
        cur.execute(upn_base_table)
        view_base = " create view f as (select s.id_key,row_number() over (partition by s.aid order by s.attendancekey) from stg_attendance as s inner join attendance on s.aid=attendance.aid)"
        cur.execute(view_base)
        upy_base_table = "update attendance set active_ind='Y' where id_key in ( select id_key from stg_attendance where id_key in (select id_key from f where row_number=1))"
        cur.execute(upy_base_table)
        cur.execute("drop view f")
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        print("check if already updated")
        conn.rollback()
conn.close()
