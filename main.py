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
    '''Знайти середній бал у групах з певного предмета'''
    r = session.query(Discipline.name,
                      Group.name,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Group) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Discipline.id).all()
    return r


def select_4():
    '''Знайти середній бал на потоці (по всій таблиці оцінок).
    :return:
    '''
    r = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).all()
    return r

def  select_5(teacher_id: int):
    """Знайти які курси читає певний викладач."""
    r = session.query(Discipline.name,
                      Teacher.fullname) \
        .select_from(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == teacher_id).all()
    return r

def select_6(group_id: int):
    """Знайти список студентів у певній групі"""
    r = session.query(Student.fullname) \
        .select_from(Student)\
        .filter(Student.group_id == group_id).all()
    return r

def select_7(group_id: int, discipline_id: int):
    """Знайти оцінки студентів у окремій групі з певного предмета"""
    r = session.query(Grade.grade,
                      Grade.date_of,
                      Student.fullname,
                      Discipline.name,
                      Group.name) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(Group.id == group_id, Discipline.id == discipline_id) \
        .order_by(Group.name).all()
    return r

def select_8(teacher_id: int):
    """Знайти середній бал, який ставить певний викладач зі своїх предметів"""
    r = session.query(Teacher.fullname,
                      Discipline.name,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Discipline.teacher_id == teacher_id).all()
    return r

def select_9(student_id):
    """Знайти список курсів, які відвідує певний студент"""
    r = session.query(Student.fullname,
                      Discipline.name)\
        .select_from(Discipline)\
        .join(Grade)\
        .join(Student)\
        .filter(Grade.student_id == student_id)\
        .group_by(Grade.discipline_id).all()
    return r

def select_10(student_id: int, teacher_id: int):
    """Список курсів, які певному студенту читає певний викладач"""
    r = session.query(Discipline.name)\
        .select_from(Discipline)\
        .join(Grade)\
        .join(Student)\
        .join(Teacher)\
        .filter(Grade.student_id == student_id, Discipline.teacher_id == teacher_id)\
        .group_by(Discipline.id).all()
    return r


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
    print(select_1())
    print(select_2(1))
    print(select_3(4))
    print(select_4())
    print(select_5(3))
    print(select_6(2))
    print(select_7(3, 5))
    print(select_8(2))
    print(select_9(7))
    print(select_10(17, 4))
    # print(select_last(1, 2))
