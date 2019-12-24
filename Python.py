import psycopg2
import os
conn = psycopg2.connect(user="postgres", password="Vai123&*", database="random", host="localhost", port=5432)
os.chdir('C:/Users/BRKReddy/Downloads/csv_files/')
cur = conn.cursor()
for file in os.listdir('.'):
    with open(file) as fl:
        lines = fl.readlines()[1:]
        for line in lines:
            values = line.strip().split(',')
            update_student_query = (f"update public.student set active_ind='N' where aid={values[0]}")
            insert_student_query = (f"insert into public.student(aid, a_name, addrid,active_ind) "
                                    f"values({values[0]}, '{values[1]}', {values[2]},'Y')"
                                    f"on conflict do nothing")
            update_address_query = (f"update public.address set active_ind='N' where addrid={values[2]}")
            insert_address_query = (f"insert into public.address(addr1, addr2, city, pstate, country, "
                                    f"contactnumber, active_ind, postalcd, aid, addrid) "
                                    f"values('{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', "
                                    f"'{values[7]}', {values[9]}, 'Y', {values[8]}, '{values[0]}', '{values[2]}')")
            update_attendance_query = (f"update public.attendance set active_ind='N' where attendancekey={values[10]}")
            insert_attendance_query = (f"insert into public.attendance(attendedyesno, active_ind, attendancekey, "
                                       f"attendancedate, aid) "
                                       f"values('{values[12]}', 'Y', {values[10]}, '{values[11]}', {values[0]})")
            insert_loading_zone_query = (f"insert into public.loading_zone(aid, a_name, addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber, attendancekey, attendancedate, attendedyesno) "
                                        f"values({values[0]}, '{values[1]}', {values[2]}, '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', '{values[7]}', {values[8]}, {values[9]}, {values[10]}, '{values[11]}', '{values[12]}')")
            insert_stage_table_query=("INSERT INTO public.stage_table(aid, a_name, addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber, attendancekey, attendancedate, attendedyesno) "
                                    "SELECT aid, a_name, addrid, addr1, addr2, city, pstate, country, postalcd, contactnumber, attendancekey, attendancedate, attendedyesno "
                                    "FROM loading_zone")
            assign= "update stage_table set action_indicator = CASE WHEN  loading_zone.aid=stage_table.aid THEN 'U' ELSE 'I' END from loading_zone"
            cur.execute(update_student_query)
            cur.execute(insert_student_query)
            cur.execute(update_address_query)
            cur.execute(insert_address_query)
            cur.execute(update_attendance_query)
            cur.execute(insert_attendance_query)
            cur.execute(insert_loading_zone_query)
            cur.execute(insert_stage_table_query)
            cur.execute(assign)
conn.commit()
conn.close()
