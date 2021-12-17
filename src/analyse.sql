create view student_satisfaction_overall as
select
    institutions.name as institution_name,
    sum(students_total * student_rating)::double/sum(students_total)::double as student_satisfaction
from
    subjects
left join
    institutions
    on institutions.id = subjects.institution
group by
    institution_name
order by
    student_satisfaction desc
;

create view student_satisfaction_by_subject as
select
    institutions.name as institution_name,
    subjects.name as subject_name,
    sum(students_total * student_rating)::double/sum(students_total)::double as student_satisfaction
from
    subjects
left join
    institutions
    on institutions.id = subjects.institution
group by
    institution_name,
    subject_name
order by
    student_satisfaction desc
;

create view research_overall as
with
    annual_subject_research as
        (select
            institution,
            year,
            sum(academic_papers) as n_papers
        from
            subjects
        group by
            institution,
            year)
select
    institutions.name as institution_name,
    sum(n_papers) as academic_papers,
    sum(n_papers)::double/sum(staff_total)::double as academic_papers_per_staff
from
    annual_subject_research
left join
    submissions
    using (institution, year)
left join
    institutions
    on institutions.id = annual_subject_research.institution
group by
    institution_name
order by
    academic_papers desc
;

create view research_by_subject as
select
    institutions.name as institution_name,
    subjects.name as subject_name,
    sum(academic_papers) as academic_papers,
    sum(academic_papers)::double/sum(staff_total)::double as academic_papers_per_staff
from
    subjects
left join
    submissions
    using (institution, year)
left join
    institutions
    on institutions.id = subjects.institution
group by
    institution_name,
    subject_name
order by
    academic_papers desc
;



