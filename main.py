from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(discipline_id: int):
    '''Знайти студента із найвищим середнім балом з певного предмета.'''
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return r

def select_3(discipline_id: int):
    r = session.query(Discipline.name,
                      Group.name,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Group) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by

"""
select s.id, s.fullname, g.grade, g.date_of
from grades g
  inner join students s on s.id = g.student_id
where g.discipline_id = 2
  and s.group_id = 2
  and g.date_of = (select max(date_of) -- находим последнее занятие для данной группы по данному предмету
                   from grades g2
                     inner join students s2 on s2.id = g2.student_id
                   where g2.discipline_id = g.discipline_id
                     and s2.group_id = s.group_id);

"""


def select_last(discipline_id, group_id):
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r


if __name__ == '__main__':
    # print(select_1())
    print(select_2(1))
    # print(select_last(1, 2))
