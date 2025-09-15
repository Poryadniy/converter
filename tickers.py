import zipfile
import re
import os
import glob


def process_archives_in_folders(root_folder, output_dir=None):
    """
    Обрабатывает архивы в папках и создает отдельный txt файл для каждой папки
    """

    # Регулярное выражение для поиска тикеров
    pattern = r'([A-Za-zА-Яа-яёЁ]+)(?=\d)'

    # Если output_dir не указан, сохраняем в текущую директорию
    if output_dir is None:
        output_dir = os.getcwd()
    else:
        # Создаем директорию, если она не существует
        os.makedirs(output_dir, exist_ok=True)

    print(f"Начинаем обработку папок в: {root_folder}")
    print(f"Файлы будут сохранены в: {output_dir}")

    # Рекурсивно ищем все подпапки
    for foldername, subfolders, filenames in os.walk(root_folder):
        # Ищем архивы в текущей папке
        archive_files = []
        for ext in ['*.zip', '*.rar', '*.7z']:
            archive_files.extend(glob.glob(os.path.join(foldername, ext)))

        # Если нашли архивы в этой папке, берем первый
        if archive_files:
            first_archive = sorted(archive_files)[0]
            folder_name = os.path.basename(foldername)

            print(f"\nОбрабатываем папку: {folder_name}")
            print(f"  Архив: {os.path.basename(first_archive)}")

            unique_tickers = set()

            try:
                with zipfile.ZipFile(first_archive, 'r') as zip_ref:
                    # Ищем все CSV файлы в архиве
                    csv_files = []
                    for file_info in zip_ref.infolist():
                        if not file_info.is_dir() and file_info.filename.endswith('.csv'):
                            csv_files.append(file_info.filename)

                    print(f"  CSV файлов: {len(csv_files)}")

                    for csv_file in csv_files:
                        try:
                            with zip_ref.open(csv_file) as file:
                                content = file.read().decode('utf-8', errors='ignore')

                                # Ищем тикеры
                                matches = re.findall(pattern, content)

                                if matches:
                                    unique_tickers.update(matches)

                        except Exception as e:
                            print(f"    Ошибка в файле {csv_file}: {e}")
                            continue

            except Exception as e:
                print(f"Ошибка архива {first_archive}: {e}")
                continue

            # Сохраняем тикеры в файл с именем папки
            if unique_tickers:
                output_file = os.path.join(output_dir, f"{folder_name}.txt")
                sorted_tickers = sorted(unique_tickers)

                with open(output_file, 'w', encoding='utf-8') as f:
                    for ticker in sorted_tickers:
                        f.write(ticker + '\n')

                print(f"  Сохранено тикеров: {len(sorted_tickers)} в {output_file}")
            else:
                print(f"  В папке {folder_name} тикеры не найдены")


# Использование:
if __name__ == "__main__":
    # Укажите путь к корневой папке с архивами
    root_folder = "D:\\Data\\Unarchived"  # Замените на ваш путь

    # Укажите путь для сохранения результатов (опционально)
    output_directory = "D:\\Data\\Tickers"  # Можно изменить или оставить None

    # Вызываем функцию
    process_archives_in_folders(root_folder, output_directory)

    print("\nОбработка завершена!")