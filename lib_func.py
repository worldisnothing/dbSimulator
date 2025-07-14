from typing import List


def is_valid_command(in_command: str) -> bool:
    """
    Проверяет корректность введенной команды

    :param in_command: введенная строка, содержащая команду в верхнем регистре
    :return: True - если команда корректна, False - если команда некорректна
    """

    # словарь, где ключ - ключевое слово команды, значение - количество аргументов после команды
    conf = {
        "SET": 2,
        "GET": 1,
        "UNSET": 1,
        "COUNTS": 1,
        "FIND": 1,
        "BEGIN": 0,
        "ROLLBACK": 0,
        "COMMIT": 0
    }

    command_args = in_command.split(' ')
    key_command = command_args[0]

    return key_command in conf and conf[key_command] == len(command_args) - 1


class DBSimulator:
    """
    Класс симулятора БД

    Обрабатывает команды:
    SET - сохраняет аргумент в базе данных.
    GET - возвращает, ранее сохраненную переменную. Если такой переменной не было сохранено, возвращает NULL
    UNSET - удаляет, ранее установленную переменную. Если значение не было установлено, не делает ничего.
    COUNTS - показывает сколько раз данные значение встречается в базе данных.
    FIND - выводит найденные установленные переменные для данного значения.
    BEGIN - начало транзакции.
    ROLLBACK - откат текущей (самой внутренней) транзакции
    COMMIT - фиксация изменений текущей (самой внутренней) транзакции
    """

    def __init__(self):
        # симулятор бд, хранит значения
        self.db = {}
        # стек снимков состояний бд для транзакций
        self._transactions = []

    def handler(self, in_command: str):
        """
        Обработчик команд
        :param in_command: введенная строка, содержащая команду в верхнем регистре
        """
        # словарь, где ключ - ключевое слово команды, значение - обработчик
        handlers = {
            "SET": self.set,
            "GET": self.get,
            "UNSET": self.unset,
            "COUNTS": self.counts,
            "FIND": self.find,
            "BEGIN": self.begin,
            "ROLLBACK": self.rollback,
            "COMMIT": self.commit
        }

        # получаем команду и аргументы
        key_cmd, args = in_command.split(' ')[0], in_command.split(' ')[1:]

        # сразу обрабатываем, т.к. все проверки пройдены в is_valid_command (в main.py)
        handlers[key_cmd](args)

    def set(self, args: List):
        """
        Записывает переданное значение в БД

        :param args: список, где первый элемент - ключ, второй - значение
        """
        k, v = args
        if self._transactions:
            if k not in self._transactions[-1]:
                self._transactions[-1][k] = self.db.get(k, None)
        self.db[k] = v

    def get(self, args: List):
        """
        Выводит значение из БД по переданному ключу. Если такого нет, выводит NULL

        :param args: список, где первый элемент - ключ возвращаемого значения
        """
        k = args[0]
        print(self.db.get(k, 'NULL'))

    def unset(self, args: List):
        """
        Удаляет значение из БД по переданному ключу

        :param args: список, где первый элемент - ключ удаляемого значения
        """
        k = args[0]
        if self._transactions:
            if k not in self._transactions[-1]:
                self._transactions[-1][k] = self.db.get(k, None)
        self.db.pop(k, None)

    def counts(self, args: List):
        """
        Выводит количество переданного значений в БД

        :param args: значение, количество корого надо посчитать
        """
        v = args[0]
        print(list(self.db.values()).count(v))

    def find(self, args: List):
        """
        Выводит найденные установленные переменные для данного значения
        :param args: значение, для которого надо найти установленные переменные
        """
        v = args[0]
        for k in self.db:
            if self.db[k] == v:
                print(k)

    def begin(self, args: List):
        """
        Начинает новую вложенную транзакцию
        """
        self._transactions.append({})

    def rollback(self, args: List[str]):
        """
        Откатывает вложенную транзакцию
        """
        if self._transactions:
            changes = self._transactions.pop()
            for k, old_val in changes.items():
                if old_val is None:
                    self.db.pop(k, None)
                else:
                    self.db[k] = old_val

    def commit(self, args: List[str]):
        """
        Закрывает ВСЕ вложенные транзакции, сохраняет состояние последней. Rollback больше не работает
        """
        if self._transactions:
            changes = self._transactions.pop()
            if self._transactions:
                for k, old_val in changes.items():
                    if k not in self._transactions[-1]:
                        self._transactions[-1][k] = old_val
