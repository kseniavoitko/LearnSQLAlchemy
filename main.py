from sqlalchemy import func, desc, select, and_

from conf.models import Teacher, Student, Discipline, Grade, Group
from conf.db import session


def select_01():
    result = (
        session.query(
            Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")
        )
        .select_from(Grade)
        .join(Student)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .limit(5)
        .all()
    )
    return result


def select_02(discipline_id: int):
    r = (
        session.query(
            Discipline.name,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Discipline.id == discipline_id)
        .group_by(Student.id, Discipline.name)
        .order_by(desc("avg_grade"))
        .limit(1)
        .all()
    )
    return r


def select_03(discipline_id: int):
    result = (
        session.query(
            Discipline.name,
            Group.name,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .join(Group)
        .filter(Discipline.id == discipline_id)
        .group_by(Group.name, Discipline.name)
        .order_by(desc("avg_grade"))
        .all()
    )
    return result


def select_04():
    result = (
        session.query(func.round(func.avg(Grade.grade), 2).label("avg_grade"))
        .select_from(Grade)
        .all()
    )
    return result


def select_05(teacher_id: int):
    result = (
        session.query(Discipline.name)
        .select_from(Discipline)
        .filter(Discipline.teacher_id == teacher_id)
        .all()
    )
    return result


def select_06(group_id: int):
    result = (
        session.query(Student.fullname)
        .select_from(Student)
        .filter(Student.group_id == group_id)
        .all()
    )
    return result


def select_07(group_id: int, discipline_id: int):
    result = (
        session.query(Student.fullname, Grade.grade)
        .select_from(Grade)
        .join(Student)
        .filter(Student.group_id == group_id, Grade.discipline_id == discipline_id)
        .order_by(Student.fullname)
        .all()
    )
    return result


def select_08(teacher_id: int):
    result = (
        session.query(
            Discipline.name, func.round(func.avg(Grade.grade), 2).label("avg_grade")
        )
        .select_from(Discipline)
        .join(Grade)
        .filter(Discipline.teacher_id == teacher_id)
        .group_by(Discipline.name)
        .all()
    )
    return result


def select_09(student_id: int):
    result = (
        session.query(
            Discipline.name,
        )
        .select_from(Grade)
        .join(Discipline)
        .filter(Grade.student_id == student_id)
        .group_by(Grade.discipline_id, Discipline.name)
        .all()
    )
    return result


def select_10(student_id: int, teacher_id: int):
    result = (
        session.query(
            Discipline.name,
        )
        .select_from(Grade)
        .join(Discipline)
        .filter(Grade.student_id == student_id, Discipline.teacher_id == teacher_id)
        .group_by(Discipline.id, Discipline.name)
        .all()
    )
    return result


def select_12(discipline_id, group_id):
    subquery = (
        select(Grade.date_of)
        .join(Student)
        .join(Group)
        .where(and_(Grade.discipline_id == discipline_id, Group.id == group_id))
        .order_by(desc(Grade.date_of))
        .limit(1)
        .scalar_subquery()
    )

    r = (
        session.query(
            Discipline.name, Student.fullname, Group.name, Grade.date_of, Grade.grade
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .join(Group)
        .filter(
            and_(
                Discipline.id == discipline_id,
                Group.id == group_id,
                Grade.date_of == subquery,
            )
        )
        .order_by(desc(Grade.date_of))
        .all()
    )
    return r


if __name__ == "__main__":
    print(select_01())
    print(select_02(1))
    print(select_03(1))
    print(select_04())
    print(select_05(1))
    print(select_06(1))
    print(select_07(1, 1))
    print(select_08(1))
    print(select_09(1))
    print(select_10(1, 1))
    print(select_12(1, 2))
