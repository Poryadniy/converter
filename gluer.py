import os
import pandas as pd
from datetime import datetime, timedelta


class FuturesConcatenator:
    def __init__(self, root_dir, rollover_days=5, debug=False):
        """
        root_dir – папка с тикерами
        rollover_days – за сколько дней до экспирации переходить на следующий контракт
        debug – если True, печатаем дополнительные логи для отладки
        """
        self.root_dir = root_dir
        self.rollover_days = rollover_days
        self.debug = debug

        # Ожидаемая структура итогового CSV (имена и порядок колонок)
        self.column_names = [
            'Date', 'Time', 'TradeID', 'TradeVolume', 'LastPrice', 'TotalVolume',
            'HighPrice', 'LowPrice', 'Nanoseconds',
            'Bid1', 'Bid2', 'Bid3', 'Bid4', 'Bid5',
            'Ask1', 'Ask2', 'Ask3', 'Ask4', 'Ask5',
            'BidVol1', 'BidVol2', 'BidVol3', 'BidVol4', 'BidVol5',
            'AskVol1', 'AskVol2', 'AskVol3', 'AskVol4', 'AskVol5'
        ]

    def parse_contract_expiry(self, contract_code):
        """Определяет календарную дату экспирации по названию контракта agYYMM"""
        try:
            code = contract_code.lower()
            yy = int("20" + code[2:4])
            mm = int(code[4:6])
            if mm == 12:
                return datetime(yy, mm, 31).date()
            else:
                next_month = datetime(yy, mm + 1, 1).date()
                return next_month - timedelta(days=1)
        except Exception:
            return None

    def find_contract_files(self, ticker_dir):
        """Ищет все csv-файлы внутри папок DAY/NIGHT"""
        contract_files = []
        for session_name in os.listdir(ticker_dir):
            if session_name.lower() not in ["day", "night"]:
                continue
            session_path = os.path.join(ticker_dir, session_name)
            if not os.path.exists(session_path):
                continue
            files = os.listdir(session_path)
            print(f"Проверяем {session_path}, файлы: {files}")
            for fname in files:
                if fname.lower().endswith(".csv"):
                    contract_code = fname.split('_')[0].lower()
                    full_path = os.path.join(session_path, fname)
                    contract_files.append((contract_code, full_path))
        return contract_files

    def _try_read_file(self, file_path):
        """
        Пытаемся прочитать файл различными разделителями.
        Сначала пробуем engine='c' (с low_memory=False), при ошибке пробуем engine='python' (без low_memory).
        Возвращает (df, sep, engine)
        """
        seps = [',', ';', '\t', '|']
        last_exc = None
        for sep in seps:
            # сначала пробуем C-движок с low_memory=False
            try:
                df_try = pd.read_csv(file_path, header=None, sep=sep, dtype=str, engine='c', low_memory=False)
                if df_try.shape[1] > 1:
                    if self.debug:
                        print(f"Файл {file_path} прочитан с sep='{sep}' engine='c' cols={df_try.shape[1]}")
                    return df_try, sep, 'c'
            except Exception as e_c:
                # если C не прокатил — пробуем python engine (без low_memory)
                try:
                    df_try = pd.read_csv(file_path, header=None, sep=sep, dtype=str, engine='python')
                    if df_try.shape[1] > 1:
                        if self.debug:
                            print(f"Файл {file_path} прочитан с sep='{sep}' engine='python' cols={df_try.shape[1]}")
                        return df_try, sep, 'python'
                except Exception as e_p:
                    last_exc = e_p
                    continue
        # если ничего не подошло — пробуем один последний раз с запятой и engine='c' (сгенерируем исключение, если не получилось)
        try:
            df_try = pd.read_csv(file_path, header=None, sep=',', dtype=str, engine='c', low_memory=False)
            return df_try, ',', 'c'
        except Exception as e:
            raise last_exc or e

    def _normalize_date_series(self, s: pd.Series):
        """
        Приводим колонку дат к строковому формату YYYYMMDD или помечаем как NA.
        Возвращаем Series строк и NA там, где не удалось распарсить.
        """
        s2 = s.astype(str).str.strip()
        # если много 8-значных чисел — считаем, что это YYYYMMDD
        if s2.str.match(r'^\d{8}$').sum() >= max(1, int(0.3 * len(s2))):
            res = s2.where(s2.str.match(r'^\d{8}$'), pd.NA)
            return res

        parsed = pd.to_datetime(s2, dayfirst=False, errors='coerce')
        res = parsed.dt.strftime('%Y%m%d')
        # пометим неопарсенные как NA
        res = res.mask(parsed.isna(), pd.NA)
        return res

    def process_ticker(self, ticker):
        """Обрабатывает один тикер"""
        ticker_dir = os.path.join(self.root_dir, ticker)
        if not os.path.isdir(ticker_dir):
            return None

        print(f"\n=== Обработка {ticker.upper()} ===")
        contract_files = self.find_contract_files(ticker_dir)
        if not contract_files:
            print("Файлы не найдены!")
            return None

        contract_data = {}
        expiry_dates = {}
        for code, path in contract_files:
            contract_data.setdefault(code, []).append(path)
            if code not in expiry_dates:
                expiry_dates[code] = self.parse_contract_expiry(code)

        sorted_contracts = sorted(
            expiry_dates.keys(),
            key=lambda x: expiry_dates.get(x) or datetime.max.date()
        )

        continuous_data = []
        used_contracts = set()

        for contract in sorted_contracts:
            files = contract_data[contract]
            files.sort()
            for file_path in files:
                try:
                    df_raw, used_sep, used_engine = self._try_read_file(file_path)
                    df_raw = df_raw.fillna('').apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

                    ncols = df_raw.shape[1]
                    if ncols < 2:
                        print(f"Файл {file_path} имеет <2 столбцов, пропускаем.")
                        continue

                    # Находим пару колонок Date/Time (соседние колонки, где одна похожа на дату)
                    date_col = None
                    time_col = None
                    max_check = min(6, ncols - 1)
                    for i in range(max_check):
                        col_date = df_raw.iloc[:, i].astype(str).str.strip()
                        col_time = df_raw.iloc[:, i + 1].astype(str).str.strip()
                        date_like = col_date.str.match(r'^\d{6,8}$')
                        time_like = col_time.str.match(r'^\d{1,9}$')
                        if date_like.sum() >= max(1, int(0.3 * len(col_date))) and time_like.sum() >= max(1, int(0.3 * len(col_time))):
                            date_col = i
                            time_col = i + 1
                            break

                    # Доп. эвристики
                    if date_col is None:
                        for i in range(min(6, ncols)):
                            if df_raw.iloc[:, i].astype(str).str.match(r'^\d{8}$').sum() >= max(1, int(0.3 * len(df_raw))):
                                date_col = i
                                break
                    if time_col is None:
                        for j in range(min(6, ncols)):
                            if df_raw.iloc[:, j].astype(str).str.match(r'^\d{1,9}$').sum() >= max(1, int(0.3 * len(df_raw))):
                                if j != date_col:
                                    time_col = j
                                    break

                    if date_col is None or time_col is None:
                        print(f"Не удалось найти Date/Time в {file_path} (sep='{used_sep}', engine='{used_engine}'), пропускаем.")
                        continue

                    if self.debug:
                        print(f"Файл {file_path}: найден date_col={date_col}, time_col={time_col}, sep='{used_sep}', engine='{used_engine}'")

                    # Нормализуем дату и время
                    date_s = df_raw.iloc[:, date_col].astype(str).str.strip()
                    time_digits = df_raw.iloc[:, time_col].astype(str).str.extract(r'(\d+)')[0].fillna('')

                    normalized_date = self._normalize_date_series(date_s)

                    mask_ms = time_digits.str.len() > 6
                    mask_hms = ~mask_ms

                    dt_series = pd.Series(pd.NaT, index=df_raw.index)

                    # HHMMSS
                    if mask_hms.any():
                        time_hms = time_digits[mask_hms].str.zfill(6)
                        dt_hms = pd.to_datetime(
                            normalized_date[mask_hms].fillna('') + ' ' + time_hms,
                            format='%Y%m%d %H%M%S',
                            errors='coerce'
                        )
                        dt_series.loc[mask_hms] = dt_hms

                    # HHMMSSmmm -> mmm -> микросекунды mmm000
                    if mask_ms.any():
                        time_ms = time_digits[mask_ms].str.zfill(9).str[:9]
                        hhmmss = time_ms.str[:-3]
                        mmm = time_ms.str[-3:]
                        micro = mmm + '000'
                        dt_ms = pd.to_datetime(
                            normalized_date[mask_ms].fillna('') + ' ' + hhmmss + micro,
                            format='%Y%m%d %H%M%S%f',
                            errors='coerce'
                        )
                        dt_series.loc[mask_ms] = dt_ms

                    valid_idx = dt_series.notna()
                    if valid_idx.sum() == 0:
                        print(f"В файле {file_path} не получилось распарсить DateTime ни для одной строки, пропускаем.")
                        continue

                    # Собираем итоговую таблицу
                    out = pd.DataFrame(index=df_raw.index[valid_idx])

                    for i, colname in enumerate(self.column_names):
                        if i < ncols:
                            out[colname] = df_raw.iloc[:, i].astype(str).str.strip()[valid_idx].astype(str)
                        else:
                            out[colname] = ''

                    out['DateTime'] = pd.to_datetime(dt_series[valid_idx])
                    out['Date'] = out['DateTime'].dt.strftime('%Y%m%d')
                    out['Time'] = out['DateTime'].dt.strftime('%H%M%S%f').str[:9]  # HHMMSSmmm

                    continuous_data.append(out.reset_index(drop=True))
                    used_contracts.add(contract)

                except Exception as e:
                    print(f"Ошибка при чтении {file_path}: {e}")

        if not continuous_data:
            print("Нет данных для обработки!")
            return None

        result_df = pd.concat(continuous_data, ignore_index=True, sort=False).sort_values('DateTime')
        print(f"- Период данных: {result_df['DateTime'].min()} - {result_df['DateTime'].max()}")
        print(f"- Использовано контрактов: {sorted(used_contracts)}")
        print(f"- Всего строк: {len(result_df):,}")

        return result_df, used_contracts

    def process_all(self, output_dir):
        """Обрабатывает все тикеры в корне"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        tickers = [d for d in os.listdir(self.root_dir) if os.path.isdir(os.path.join(self.root_dir, d))]
        print("Найденные тикеры:", tickers)

        total_stats = []
        for ticker in tickers:
            result = self.process_ticker(ticker)
            if result is None:
                continue
            df, used = result

            ticker_out_dir = os.path.join(output_dir, ticker)
            os.makedirs(ticker_out_dir, exist_ok=True)

            # Сохраняем CSV с колонками в нужном порядке (без дополнительного 'Contract')
            out_df = df[self.column_names]
            out_file = os.path.join(ticker_out_dir, f"{ticker}.csv")
            out_df.to_csv(out_file, index=False, sep=';', encoding='utf-8-sig')
            print(f"Файл сохранен: {out_file}")

            total_stats.append({
                "Ticker": ticker,
                "StartDate": df["DateTime"].min(),
                "EndDate": df["DateTime"].max(),
                "Rows": len(df),
                "Contracts": len(used)
            })

        if total_stats:
            stats_df = pd.DataFrame(total_stats)
            stats_path = os.path.join(output_dir, "summary_stats.csv")
            stats_df.to_csv(stats_path, index=False, sep=';', encoding='utf-8-sig')
            print(f"\nСводная статистика сохранена в {stats_path}")


'''if __name__ == "__main__":
    ROOT_DIR = "D:\\Data\\TickersData"
    OUTPUT_DIR = "D:\\Data\\GluedData"
    ROLLOVER_DAYS = 5

    print("=== Склейка фьючерсов с заданной структурой данных ===\n")
    concatenator = FuturesConcatenator(ROOT_DIR, ROLLOVER_DAYS, debug=False)
    concatenator.process_all(output_dir=OUTPUT_DIR)'''
