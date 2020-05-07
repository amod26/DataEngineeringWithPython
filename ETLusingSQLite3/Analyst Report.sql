
select A.batch_id, count(A.id) as "No of students per batch"  from (
                  select s.batch_id, s.id, count(n.id) as "No of lesson per student" 
                  from students s
                           join New_data n
                                on s.id = n.student_id
                  group by 1,2
                  having "No of lesson per student" >= 3 
              ) as A
group by  1
order by 2 desc

-- This would give us the the batches with the most students that have completed at least 3 lessons.