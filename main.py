# main.py
import os

from globalUnarchiver import NestedArchiveExtractor
from unarchiver import find_all_tickers, extract_and_organize_sequential
from gluer import FuturesConcatenator
from converter import FinamTxtCandleGenerator

def main():
    # Шаг 1: Разархивация
    print("🚀 Запуск разархивации...")
    extractor = NestedArchiveExtractor()
    input_directory = "D:\\Data\\ChinaData"
    unarchived_directory = "D:\\Data\\Unarchived"  # Output для разархивации, input для организации

    try:
        extractor.process_directory(input_directory, unarchived_directory)
    except Exception as e:
        print(f"Произошла ошибка в разархивации: {e}")
        return

    # Шаг 2: Поиск тикеров и организация
    print("\n🚀 Запуск организации по тикерам...")
    tickers_directory = "D:\\Data\\TickersData"  # Output для организации, input для склейки

    if not os.path.exists(unarchived_directory):
        print("❌ Директория с разархивированными файлами не существует!")
        return

    tickers = find_all_tickers(unarchived_directory)

    print("=" * 50)
    print(f"📊 Найдено уникальных тикеров: {len(tickers)}")
    print("📋 Список тикеров:")
    for i, ticker in enumerate(tickers, 1):
        print(f"{i:3d}. {ticker}")

    if tickers:
        extract_and_organize_sequential(unarchived_directory, tickers_directory, tickers)
    else:
        print("❌ Тикеры не найдены!")
        return

    # Шаг 3: Склейка данных по контрактам
    print("\n🚀 Запуск склейки данных...")
    glued_directory = "D:\\Data\\GluedData"  # Output для склейки, input для генерации свечей
    rollover_days = 5
    debug_mode = False

    concatenator = FuturesConcatenator(tickers_directory, rollover_days, debug=debug_mode)
    concatenator.process_all(output_dir=glued_directory)

    # Шаг 4: Генерация свечей в TXT-формате
    print("\n🚀 Запуск генерации свечей...")
    candle_directory = "D:\\Data\\CandleData"  # Output для свечей
    timeframes = ['Min1', 'Min5', 'Min15', 'Hour1', 'Hour4', 'Day']  # Можно изменить список

    generator = FinamTxtCandleGenerator(glued_directory, candle_directory, timeframes)
    generator.process_all()

    # Финальная пауза
    input("Нажмите Enter для выхода...")

if __name__ == "__main__":
    main()