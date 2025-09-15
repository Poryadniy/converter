import os
import pandas as pd


class FinamTxtCandleGenerator:
    def __init__(self, input_dir, output_dir,
                 timeframes=('Min1', 'Min5', 'Min15', 'Hour1', 'Hour4', 'Day')):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.timeframes = list(timeframes)
        self.timeframe_mapping = {
            'Min1': '1min',
            'Min5': '5min',
            'Min15': '15min',
            'Hour1': '1h',
            'Hour4': '4h',
            'Day': '1D'
        }

    def _find_csv_in_folder(self, folder):
        for fname in os.listdir(folder):
            if fname.lower().endswith('.csv'):
                return os.path.join(folder, fname)
        return None

    def load_continuous_data(self, file_path):
        if not os.path.exists(file_path):
            print(f"Файл {file_path} не найден!")
            return None

        try:
            df = pd.read_csv(file_path, sep=';', header=0, low_memory=False)
        except Exception as e:
            print(f"Ошибка чтения {file_path}: {e}")
            return None

        # Найти колонки даты и времени
        cols_lower = [c.lower() for c in df.columns]
        date_col = next((df.columns[i] for i, c in enumerate(cols_lower) if 'date' in c), None)
        time_col = next((df.columns[i] for i, c in enumerate(cols_lower) if 'time' in c), None)

        if date_col is None or time_col is None:
            print(f"В файле {file_path} нет колонок Date/Time")
            return None

        date_s = df[date_col].astype(str).str.strip()
        time_s = df[time_col].astype(str).str.strip()

        # Оставляем только цифры и убираем лишние .0
        time_s = time_s.str.replace(r'\D', '', regex=True)
        time_s = time_s.str.replace(r'\.0+$', '', regex=True)
        # Берём последние 6 цифр (HHMMSS) и заполняем нулями спереди, если короткое
        time_s = time_s.apply(lambda x: x[-6:].zfill(6) if len(x) >= 6 else x.zfill(6))

        combined = date_s + ' ' + time_s
        df['DateTime'] = pd.to_datetime(combined, errors='coerce')

        if df['DateTime'].isna().all():
            print(f"Не удалось распознать даты в файле {file_path}")
            return None

        return df

    def _detect_price_volume_cols(self, df):
        cols_map = {c.lower(): c for c in df.columns}
        price_candidates = ['lastprice', 'last_price', 'last', 'price', 'tradeprice', 'trade_price', 'close']
        volume_candidates = ['totalvolume', 'total_volume', 'volume', 'qty', 'quantity', 'vol']

        price_col = next((cols_map[p] for p in price_candidates if p in cols_map), None)
        vol_col = next((cols_map[v] for v in volume_candidates if v in cols_map), None)

        return price_col, vol_col

    def generate_candles(self, df, timeframe):
        if timeframe not in self.timeframe_mapping:
            print(f"Неизвестный таймфрейм: {timeframe}")
            return None

        df = df.dropna(subset=['DateTime']).copy()
        if df.empty:
            print("Нет валидных дат для ресемплинга.")
            return None

        # сортировка по дате
        df = df.sort_values('DateTime')

        price_col, vol_col = self._detect_price_volume_cols(df)
        if price_col is None:
            print("Не найдена колонка цены.")
            return None
        if vol_col is None:
            df['TotalVolume_detected'] = 0
            vol_col = 'TotalVolume_detected'

        # Приведение к числовому типу
        df.loc[:, price_col] = pd.to_numeric(df[price_col], errors='coerce')
        df.loc[:, vol_col] = pd.to_numeric(df[vol_col], errors='coerce').fillna(0)

        # Ресемплинг
        df = df.set_index('DateTime')
        agg = df.resample(self.timeframe_mapping[timeframe]).agg({
            price_col: ['first', 'max', 'min', 'last'],
            vol_col: 'sum'
        })

        agg.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        candles = agg.reset_index()
        candles = candles.dropna(subset=['Open', 'High', 'Low', 'Close'])
        if candles.empty:
            print(f"Нет свечей для таймфрейма {timeframe}")
            return None

        candles['OpenInterest'] = 0

        if timeframe == '1D':
            candles['DateTime'] = candles['DateTime'].dt.strftime('%Y%m%d,000000')
        else:
            candles['DateTime'] = candles['DateTime'].dt.strftime('%Y%m%d,%H%M%S')

        return candles[['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']]

    def save_to_txt(self, df, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            for _, row in df.iterrows():
                open_p = 0.0 if pd.isna(row['Open']) else float(row['Open'])
                high_p = 0.0 if pd.isna(row['High']) else float(row['High'])
                low_p = 0.0 if pd.isna(row['Low']) else float(row['Low'])
                close_p = 0.0 if pd.isna(row['Close']) else float(row['Close'])
                vol = 0 if pd.isna(row['Volume']) else int(row['Volume'])
                oi = int(row.get('OpenInterest', 0))
                line = f"{row['DateTime']},{open_p:.2f},{high_p:.2f},{low_p:.2f},{close_p:.2f},{vol},{oi}\n"
                f.write(line)

    def process_symbol(self, ticker_folder_name):
        folder = os.path.join(self.input_dir, ticker_folder_name)
        csv_path = self._find_csv_in_folder(folder)
        if csv_path is None:
            print(f"В папке {folder} нет CSV-файла. Пропускаю.")
            return

        df = self.load_continuous_data(csv_path)
        if df is None:
            return

        print(f"\nГенерация свечей для {ticker_folder_name.upper()} из файла {os.path.basename(csv_path)}")

        for tf in self.timeframes:
            candles = self.generate_candles(df, tf)
            if candles is None:
                continue
            out_file = os.path.join(self.output_dir, ticker_folder_name, tf,
                                    f"{ticker_folder_name}_{tf}.txt")
            self.save_to_txt(candles, out_file)
            print(f"  {tf}: {len(candles):,} строк -> {out_file}")

    def process_all(self):
        if not os.path.isdir(self.input_dir):
            print(f"Каталог {self.input_dir} не найден.")
            return

        tickers = [d for d in os.listdir(self.input_dir)
                   if os.path.isdir(os.path.join(self.input_dir, d))]

        if not tickers:
            print(f"В каталоге {self.input_dir} не найдено папок с тикерами.")
            return

        print("=== Генерация TXT-файлов в формате Finam ===")
        for t in tickers:
            try:
                self.process_symbol(t)
            except Exception as e:
                print(f"Ошибка при обработке {t}: {e}")


'''if __name__ == '__main__':
    INPUT_DIR = r'D:\Data\GluedData'
    OUTPUT_DIR = r'D:\Data\CandleData'
    TIMEFRAMES = ['Min1', 'Min5', 'Min15', 'Hour1', 'Hour4', 'Day']
    
    gen = FinamTxtCandleGenerator(INPUT_DIR, OUTPUT_DIR, TIMEFRAMES)
    gen.process_all()'''
