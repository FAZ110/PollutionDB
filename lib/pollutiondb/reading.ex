defmodule Pollutiondb.Reading do
  use Ecto.Schema
  import Ecto.Changeset

  alias Pollutiondb.{Repo, Station}
  import Ecto.Query, only: [from: 2]

  schema "readings" do
    field(:date, :date)
    field(:time, :time)
    field(:type, :string)
    field(:value, :float)

    belongs_to(:station, Pollutiondb.Station)

    timestamps()
  end

  @doc """
  Change-set dla tabeli `readings`.
  Dodajemy `foreign_key_constraint/3`, aby
  w razie nieistniejącego `station_id` otrzymać
  {:error, changeset} zamiast wyjątku.
  """
  def changeset(reading, attrs) do
    reading
    |> cast(attrs, [:date, :time, :type, :value, :station_id])
    |> validate_required([:date, :time, :type, :value, :station_id])
    |> validate_number(:value, greater_than_or_equal_to: 0.0)
    |> foreign_key_constraint(:station_id)
  end

  @doc """
  Wstawia nowy odczyt „na teraz” – używa `Date.utc_today/0` i `Time.utc_now/0`.

  Może być wywołane albo z `%Station{id: st_id}`:
      iex> Reading.add_now(station_struct, "PM10", 12.3)

  albo bezpośrednio z samym `station_id` (liczbą):
      iex> Reading.add_now(6, "PM10", 12.3)
  """
  def add_now(%Station{id: st_id}, type, value)
      when is_binary(type) and is_number(value) do
    attrs = %{
      date: Date.utc_today(),
      time: Time.utc_now(),
      type: type,
      value: value,
      station_id: st_id
    }

    %__MODULE__{}
    |> changeset(attrs)
    |> Repo.insert()
  end

  def add_now(station_id, type, value)
      when is_integer(station_id) and is_binary(type) and is_number(value) do
    attrs = %{
      date: Date.utc_today(),
      time: Time.utc_now(),
      type: type,
      value: value,
      station_id: station_id
    }

    %__MODULE__{}
    |> changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Wstawia dowolny odczyt na dowolną datę i czas.

  Argumenty:
    - station_id :: integer (klucz obcy do tabeli `stations`)
    - date       :: %Date{} (np. ~D[2024-02-10])
    - time       :: %Time{} (np. ~T[09:00:00])
    - type       :: string (np. "PM10", "CO2" itp.)
    - value      :: float (np. 27.92)

  Zwraca:
    - {:ok, %Reading{…}} przy poprawnym wstawieniu
    - {:error, changeset} jeśli walidacja/klucz obcy nie przejdzie
  """
  def add(station_id, %Date{} = date, %Time{} = time, type, value)
      when is_integer(station_id) and is_binary(type) and is_number(value) do
    attrs = %{
      date: date,
      time: time,
      type: type,
      value: value,
      station_id: station_id
    }

    %__MODULE__{}
    |> changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Overload, który przyjmuje `%Station{id: st_id}` zamiast `station_id`:
      iex> Reading.add(%Station{id: 6}, ~D[2024-02-10], ~T[09:00:00], "PM10", 27.92)
  """
  def add(%Station{id: st_id}, %Date{} = date, %Time{} = time, type, value) do
    add(st_id, date, time, type, value)
  end

  @doc """
  Znajduje wszystkie odczyty z dokładnie podaną datą (%Date{}).
  """
  def find_by_date(date) when is_struct(date, Date) do
    query =
      from(r in __MODULE__,
        where: r.date == ^date
      )

    Repo.all(query)
  end
end
