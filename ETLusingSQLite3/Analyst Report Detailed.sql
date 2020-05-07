
--Detailed view of Batches that have students who have taken more that 3 lessons
select a.batch_id, a.id as "student_id", s.name as "student_name",n.id as "lesson_id", n.title as "lesson_name", n.received,n.total
from (
	  select s.batch_id, s.id, count(n.id) as "No of lesson per student"
	  from students s
			   join New_data n
					on s.id = n.student_id
	  group by 1,2
	  having "No of lesson per student" >= 3
  ) as A 
  join students s on s.id=a.id 
  join New_data n on a.id=n.student_id
group by 1,2,4
order by 1,2,4,6 DESC