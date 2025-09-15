import zipfile
import tarfile
import os
import shutil
from pathlib import Path
import tempfile


class NestedArchiveExtractor:
    def __init__(self, supported_formats=None):
        """
        Инициализация экстрактора архивов

        :param supported_formats: Список поддерживаемых форматов архивов
        """
        self.supported_formats = supported_formats or ['.zip', '.tar', '.gz', '.bz2']

    def is_archive(self, filename):
        """Проверяет, является ли файл архивом поддерживаемого формата"""
        return any(filename.lower().endswith(fmt) for fmt in self.supported_formats)

    def extract_archive(self, archive_path, extract_to):
        """Извлекает архив в указанную директорию"""
        archive_path = Path(archive_path)

        try:
            if archive_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
            elif archive_path.suffix.lower() in ['.tar', '.gz', '.bz2']:
                mode = 'r'
                if archive_path.suffix.lower() in ['.gz', '.bz2']:
                    mode += ':gz' if archive_path.suffix.lower() == '.gz' else ':bz2'

                with tarfile.open(archive_path, mode) as tar_ref:
                    tar_ref.extractall(extract_to)
            else:
                print(f"Не поддерживаемый формат архива: {archive_path}")
                return False
            return True
        except Exception as e:
            print(f"Ошибка при извлечении {archive_path}: {e}")
            return False

    def process_initial_archive(self, archive_path, output_base_dir):
        """
        Обрабатывает один начальный архив

        :param archive_path: Путь к начальному архиву
        :param output_base_dir: Базовая директория для результатов
        """
        archive_path = Path(archive_path)
        archive_name = archive_path.stem  # Имя архива без расширения

        # Создаем целевую папку с именем начального архива
        target_dir = Path(output_base_dir) / archive_name
        target_dir.mkdir(parents=True, exist_ok=True)

        # Создаем временную директорию для извлечения
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Извлекаем начальный архив
            if not self.extract_archive(archive_path, temp_path):
                return

            # Ищем папку с именем исходного архива
            expected_folder_name = archive_name
            source_folder = None

            # Проверяем, есть ли папка с таким именем в корне извлечения
            for item in temp_path.iterdir():
                if item.is_dir() and item.name == expected_folder_name:
                    source_folder = item
                    break

            if not source_folder:
                print(f"Папка '{expected_folder_name}' не найдена в архиве {archive_path.name}")
                # Можно добавить логику для обработки других случаев
                return

            # Ищем все архивы в найденной папке
            archives_found = 0
            for item in source_folder.rglob('*'):
                if item.is_file() and self.is_archive(item.name):
                    # Перемещаем архив в целевую директорию
                    destination = target_dir / item.name
                    shutil.copy2(item, destination)
                    archives_found += 1
                    print(f"Найден и перемещен: {item.name}")

            print(f"Обработан архив {archive_path.name}. Найдено архивов: {archives_found}")

    def process_directory(self, input_dir, output_base_dir=None):
        """
        Обрабатывает все архивы в указанной директории

        :param input_dir: Директория с начальными архивами
        :param output_base_dir: Базовая директория для результатов (по умолчанию ./output)
        """
        input_path = Path(input_dir)

        if not output_base_dir:
            output_base_dir = input_path.parent / "output"

        output_path = Path(output_base_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        print(f"Начинаем обработку директории: {input_path}")
        print(f"Результаты будут сохранены в: {output_path}")
        print("-" * 50)

        # Ищем все поддерживаемые архивы во входной директории
        archives_to_process = []
        for fmt in self.supported_formats:
            archives_to_process.extend(input_path.glob(f"*{fmt}"))

        print(f"Найдено архивов для обработки: {len(archives_to_process)}")

        # Обрабатываем каждый архив
        for archive_path in archives_to_process:
            print(f"\nОбрабатываем: {archive_path.name}")
            self.process_initial_archive(archive_path, output_path)

        print("\n" + "=" * 50)
        print("Обработка завершена!")


'''def main():
    """Пример использования скрипта"""
    # Создаем экземпляр экстрактора
    extractor = NestedArchiveExtractor()

    # Указываем пути
    input_directory = "D:\\Data\\ChinaData"
    output_directory = "D:\\Data\\Unarchived"

    if not output_directory:
        output_directory = None

    # Запускаем обработку
    try:
        extractor.process_directory(input_directory, output_directory)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        input("Нажмите Enter для выхода...")


if __name__ == "__main__":
    main()'''