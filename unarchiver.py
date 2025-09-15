import zipfile
import re
import os
import shutil
import time


def find_all_tickers(root_directory):
    """Находит все тикеры из имен CSV файлов в архивах"""
    pattern = r'^([^\d]+)(?=\d)'  # Все что до первой цифры в имени файла
    all_tickers = set()
    archive_count = 0
    csv_count = 0

    print("🔍 Поиск тикеров в архивах...")
    start_time = time.time()

    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith('.zip'):
                archive_count += 1
                try:
                    with zipfile.ZipFile(os.path.join(root, file), 'r') as zip_ref:
                        for zip_file in zip_ref.namelist():
                            if zip_file.endswith('.csv'):
                                csv_count += 1
                                # Берем только имя файла без пути
                                filename = os.path.basename(zip_file)
                                match = re.match(pattern, filename)
                                if match:
                                    ticker = match.group(1).strip()
                                    if ticker:  # Проверяем что не пустая строка
                                        all_tickers.add(ticker)
                except Exception as e:
                    print(f"⚠️  Ошибка при чтении архива {file}: {e}")
                    continue

    end_time = time.time()
    print(f"✅ Проверено архивов: {archive_count}, CSV файлов: {csv_count}")
    print(f"⏱️  Время поиска: {end_time - start_time:.2f} секунд")

    return sorted(list(all_tickers))


def extract_and_organize_sequential(root_directory, output_directory, tickers_list):
    """Разархивирует и организует файлы по тикерам последовательно"""
    print("\n📦 Начинаем обработку архивов...")
    start_time = time.time()

    # Создаем директории для тикеров
    print("📁 Создание структуры папок...")
    for ticker in tickers_list:
        os.makedirs(os.path.join(output_directory, ticker, 'DAY'), exist_ok=True)
        os.makedirs(os.path.join(output_directory, ticker, 'NIGHT'), exist_ok=True)
    print(f"✅ Создано папок для {len(tickers_list)} тикеров")

    # Собираем все архивы для обработки
    archive_list = []
    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith('.zip'):
                archive_list.append((root, file))

    print(f"📦 Найдено архивов для обработки: {len(archive_list)}")

    # Обрабатываем архивы последовательно
    total_processed_files = 0
    successful_archives = 0
    failed_archives = 0

    for i, (root, file) in enumerate(archive_list, 1):
        print(f"🔹 Обрабатываем архив {i}/{len(archive_list)}: {file}")

        try:
            with zipfile.ZipFile(os.path.join(root, file), 'r') as zip_ref:
                # Создаем временную директорию с уникальным именем
                temp_dir = os.path.join(root, f'temp_extract_{i}')
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

                zip_ref.extractall(temp_dir)

                archive_processed = 0
                for extract_root, _, extract_files in os.walk(temp_dir):
                    for csv_file in extract_files:
                        if csv_file.endswith('.csv'):
                            csv_path = os.path.join(extract_root, csv_file)
                            filename = os.path.basename(csv_file)

                            # Определяем тикер из имени файла
                            match = re.match(r'^([^\d]+)(?=\d)', filename)
                            if match:
                                file_ticker = match.group(1).strip()
                                if file_ticker in tickers_list:
                                    # Определяем DAY/NIGHT
                                    data_type = 'NIGHT' if 'NIGHT' in extract_root.upper() else 'DAY'
                                    dest_dir = os.path.join(output_directory, file_ticker, data_type)
                                    os.makedirs(dest_dir, exist_ok=True)

                                    # Копируем файл
                                    shutil.copy2(csv_path, os.path.join(dest_dir, csv_file))
                                    archive_processed += 1
                                    total_processed_files += 1

                # Очищаем временную директорию
                shutil.rmtree(temp_dir)

                successful_archives += 1
                print(f"✅ Обработано: {file} (файлов: {archive_processed})")

                # Выводим прогресс каждые 10 архивов
                if i % 10 == 0 or i == len(archive_list):
                    print(f"📊 Прогресс: {i}/{len(archive_list)} архивов | Файлов: {total_processed_files}")

        except Exception as e:
            failed_archives += 1
            print(f"⚠️  Ошибка в архиве {file}: {e}")

            # Убедимся что временная директория очищена
            temp_dir = os.path.join(root, f'temp_extract_{i}')
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except:
                    pass

    end_time = time.time()
    print(f"\n🎉 Обработка завершена!")
    print(f"📦 Успешных архивов: {successful_archives}")
    print(f"❌ Неудачных архивов: {failed_archives}")
    print(f"📄 Всего обработано CSV файлов: {total_processed_files}")
    print(f"⏱️  Общее время выполнения: {end_time - start_time:.2f} секунд")

    return total_processed_files


'''def main():
    print("🚀 Запуск скрипта для организации данных по тикерам")
    print("=" * 50)

    root_directory = "D:\\Data\\Unarchived"
    output_directory = "D:\\Data\\TickersData"

    if not os.path.exists(root_directory):
        print("❌ Директория не существует!")
        return

    print("=" * 50)

    # Находим все тикеры
    tickers = find_all_tickers(root_directory)

    print("=" * 50)
    print(f"📊 Найдено уникальных тикеров: {len(tickers)}")
    print("📋 Список тикеров:")
    for i, ticker in enumerate(tickers, 1):
        print(f"{i:3d}. {ticker}")

    if tickers:
        print("=" * 50)
        # Организуем файлы последовательно
        extract_and_organize_sequential(root_directory, output_directory, tickers)
    else:
        print("❌ Тикеры не найдены!")


if __name__ == "__main__":
    main()'''