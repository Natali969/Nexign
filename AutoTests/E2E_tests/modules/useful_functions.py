# Раскладывает CDR-файл на записи, которые записывваются в список
def read_and_parse_file(filepath):
    try:
        with open(filepath, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        print(f"Ошибка: файл '{filepath}' не найден.")
        return []
    data = []
    for line in lines:
        line = line.strip()  # удаляем пробелы в начале и в конце
        if line: # пропускаем пустые строки
            elements = line.split(',')
            data.append(elements)
    return data

# хронология дат в списке соблюдена или нет
def dates_not_increasing(dates):
  """Проверяет, не идут ли даты строго по возрастанию."""
  for i in range(1, len(dates)):
    if dates[i] >= dates[i-1]:
      return True  # Даты идут по возрастанию или есть равные даты
  return False  # Даты не идут строго по возрастанию