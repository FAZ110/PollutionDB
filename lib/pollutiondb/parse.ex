defmodule Pollutiondb.Parse do
  @moduledoc """
  Moduł służący do parsowania linii CSV i wstawiania stacji + odczytów
  do bazy za pomocą modułów `Pollutiondb.Station` i `Pollutiondb.Reading`.
  """

  alias Pollutiondb.{Repo, Station, Reading}

  @doc """
  Parsuje jedną linię CSV w formacie:
    "2024-02-10T09:00:00.000Z;PM10;35.56;57570;Polska, Kraków, Floriana Straszewskiego;50.057224,19.933157"

  Zwraca mapę:
    %{
       id:         "57570",
       name:       "Polska, Kraków, Floriana Straszewskiego",
       coords:     {50.057224, 19.933157},
       type:       "PM10",
       value:      35.56,
       date:       {2024, 2, 10},    # surowa forma {rok, miesiąc, dzień}
       time:       {9, 0, 0}         # surowa forma {godzina, minuta, sekunda}
    }
  """
  def parse_line(line) do
    [ts, type, value_str, id, name, coords] = String.split(line, ";")

    # Parsujemy wartość:
    value = String.to_float(value_str)

    # Parsujemy współrzędne:
    {lat, lon} =
      coords
      |> String.trim()
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_float/1)
      |> List.to_tuple()

    # Parsujemy datę i czas: "2024-02-10T09:00:00.000Z"
    # Obetnij ".000Z" (albo po prostu "Z"):
    ts_clean = String.trim_trailing(ts, "Z")
    # Od TS w formacie "2024-02-10T09:00:00.000"
    case NaiveDateTime.from_iso8601(ts_clean) do
      {:ok, naive_dt} ->
        date = NaiveDateTime.to_date(naive_dt)
        time = NaiveDateTime.to_time(naive_dt)

        %{
          id: id,
          name: name,
          coords: {lat, lon},
          type: type,
          value: value,
          date: date,
          time: time
        }

      {:error, _reason} ->
        # Jeżeli format daty jest niepoprawny, zwracamy `nil`, aby komponent wyżej mógł to zignorować
        nil
    end
  end

  @doc """
  Dla podanej linii CSV:
    1. Parsuje linię przy pomocy `parse_line/1`.
    2. Jeżeli `parse_line/1` zwróci `nil`, pomija linię.
    3. W przeciwnym razie:
       a) Sprawdza, czy stacja o podanych coords (lat, lon) już istnieje:
          - jeżeli nie, wstawia nową stację przy pomocy `Station.add(%{name: name, lon: lon, lat: lat})`.
          - jeżeli tak, pobiera istniejącą instancję `Station`.
       b) Wstawia odczyt w tabeli `readings` przez `Reading.add(station_id, date, time, type, value)`.
  """
  def send_to_db(%{
        name: name,
        coords: {lat, lon},
        date: %Date{} = date,
        time: %Time{} = time,
        type: type,
        value: value
      }) do
    # 1) Znajdź stację po współrzędnych:
    station =
      case Station.get_by_coords(lat, lon) do
        nil ->
          # Jeśli nie ma, to utwórz:
          {:ok, st} = Station.add(%{name: name, lon: lon, lat: lat})
          st

        %Station{} = st ->
          st
      end

    # 2) Wstaw odczyt
    case Reading.add(station.id, date, time, type, value) do
      {:ok, _r} ->
        :ok

      {:error, changeset} ->
        IO.puts(
          "  [WARN] Nie udało się wstawić odczytu: #{inspect(changeset.errors)} dla stacji #{station.name}"
        )

        :error
    end
  end

  @doc """
  Lokalne wczytanie całego pliku CSV do bazy danych.

  Parametr `file_path` powinien być pełną ścieżką do pliku,
  np. Path.join([:code.priv_dir(:pollutiondb), "repo", "data", "AirlyData-ALL-50k.csv"]).

  Strategia:
    1. Czyści tabele `stations` i `readings` przez `Repo.delete_all/1`.
    2. Czyta cały plik jako strumień wierszy (File.stream!/1).
    3. Dla każdej linii:
         a) Parsuje ją poprzez `parse_line/1`.
         b) Jeżeli `parse_line/1` daje `nil`, pomija linię.
         c) W przeciwnym razie wywołuje `send_to_db/1`.
    4. Na ekran wypisuje prosty postęp co 1_000 wierszy.
  """
  def load_csv_data(file_path) do
    IO.puts("=== ROZPOCZYNAM ŁADOWANIE DANYCH Z CSV ===")

    # 1) Wyczyszczenie tabel
    Repo.delete_all(Reading)
    Repo.delete_all(Station)

    # 2) Odczyt pliku jako strumień
    total_lines =
      File.stream!(file_path)
      |> Enum.count()

    IO.puts("W pliku znajduje się #{total_lines} linii. Zaczynam przetwarzanie…")

    File.stream!(file_path)
    |> Stream.with_index(1)
    |> Stream.each(fn {line, idx} ->
      if rem(idx, 1000) == 0 do
        IO.puts("  Przetworzono #{idx}/#{total_lines} wierszy…")
      end

      line = String.trim(line)

      case parse_line(line) do
        nil ->
          # Jeżeli parse_line zwróci nil, oznacza błąd formatu – ignorujemy
          :ok

        parsed_map ->
          send_to_db(parsed_map)
      end
    end)
    |> Stream.run()

    IO.puts("=== WSZYSTKIE DANE ZAŁADOWANE ===")
  end

  @doc """
  Testowa funkcja benchmarkująca czas ładowania (podział na dodawanie stacji i dodawanie pomiarów).

  Uwaga: w powyższej implementacji `load_csv_data/1` wstawia je wszystkie „na raz”.
  Jeżeli chcesz zmierzyć oddzielnie etap dodawania stacji i etap dodawania odczytów,
  możesz wydzielić najpierw sekwencję tworzenia wszystkich unikalnych stacji, a
  potem etap wstawiania odczytów. Poniżej przykład alternatywnego podejścia.
  """
  def benchmark_csv_loading(file_path) do
    IO.puts("=== ROZPOCZYNAM BENCHMARK ŁADOWANIA DANYCH ===")

    # 1) Wyczyszczenie tabel
    Repo.delete_all(Reading)
    Repo.delete_all(Station)

    # 2) Wczytujemy plik do listy linii
    lines = File.read!(file_path) |> String.trim() |> String.split("\n")
    total_lines = length(lines)
    IO.puts("Plik ma #{total_lines} wierszy. Zbieram unikalne stacje…")

    # 3) Parsujemy każdą linię tylko po to, żeby uzyskać mapy i stacje
    parsed_data =
      lines
      |> Enum.map(&parse_line/1)
      |> Enum.filter(&(&1 != nil))

    # 4) Wyciągamy unikalne stacje (po nazwie i współrzędnych):
    unique_stations =
      parsed_data
      |> Enum.map(fn %{name: name, coords: coords} ->
        {name, coords}
      end)
      |> Enum.uniq()

    IO.puts("Znaleziono #{length(unique_stations)} unikalnych stacji.")

    # 5) Mierzymy czas wstawiania stacji
    {stations_time, stations_added} =
      :timer.tc(fn ->
        Enum.reduce(unique_stations, 0, fn {name, {lat, lon}}, acc ->
          case Station.get_by_coords(lat, lon) do
            nil ->
              case Station.add(%{name: name, lon: lon, lat: lat}) do
                {:ok, _} -> acc + 1
                {:error, _} -> acc
              end

            %Station{} ->
              acc
          end
        end)
      end)

    IO.puts("  Dodano #{stations_added} stacji w czasie #{stations_time / 1_000_000} s.")

    # 6) Mierzymy czas wstawiania pomiarów
    {measurements_time, measurements_added} =
      :timer.tc(fn ->
        Enum.reduce(parsed_data, 0, fn %{
                                         name: name,
                                         date: date,
                                         time: time,
                                         type: type,
                                         value: value,
                                         coords: {lat, lon}
                                       },
                                       acc ->
          # Pobierz stację (już powinna być wstawiona):
          %Station{id: st_id} = Station.get_by_coords(lat, lon)

          case Reading.add(st_id, date, time, type, value) do
            {:ok, _} -> acc + 1
            {:error, _} -> acc
          end
        end)
      end)

    IO.puts(
      "  Dodano #{measurements_added} pomiarów w czasie #{measurements_time / 1_000_000} s."
    )

    total_time = (stations_time + measurements_time) / 1_000_000
    total_added = stations_added + measurements_added

    IO.puts("=== PODSUMOWANIE BENCHMARKU ===")
    IO.puts("Czas tworzenia stacji: #{stations_time / 1_000_000} s")
    IO.puts("Czas tworzenia pomiarów: #{measurements_time / 1_000_000} s")
    IO.puts("Łączny czas: #{total_time} s")
    IO.puts("Łącznie dodanych rekordów: #{total_added}")

    %{
      stations_time_sec: stations_time / 1_000_000,
      stations_added: stations_added,
      measurements_time_sec: measurements_time / 1_000_000,
      measurements_added: measurements_added,
      total_time_sec: total_time,
      total_added: total_added
    }
  end
end
