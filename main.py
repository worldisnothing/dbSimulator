from lib_func import is_valid_command, DBSimulator

end = "END"

if __name__ == '__main__':
    simDB = DBSimulator()
    print("Please enter a command...")
    in_command = ""
    while True:
        # получаем команду, обработаем EOF и прерывание
        try:
            in_command = str(input())
        except (EOFError, KeyboardInterrupt):
            print("Bye!")
            break
        # удаляем пробелы слева и справа и переводим в верхний регистр
        in_command = in_command.upper().strip().lstrip()

        # проверяем END и завершаем программу
        if in_command == end:
            break

        # проверяем корректность команды и выполняем ее
        if is_valid_command(in_command):
            simDB.handler(in_command)
        else:
            print(f"Invalid command \n>>> {in_command}\nPlease try again!")