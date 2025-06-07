defmodule Pollutiondb.Station do
  use Ecto.Schema
  import Ecto.Changeset
  alias Pollutiondb.Repo
  alias __MODULE__
  require Ecto.Query

  schema "stations" do
    field(:name, :string)
    field(:lon, :float)
    field(:lat, :float)

    has_many(:readings, Pollutiondb.Reading)

    timestamps()
  end

  @doc """
  Dodaje nową stację na podstawie mapy z kluczami :name, :lon, :lat
  """
  def add(station_attrs) do
    %Station{}
    |> changeset(station_attrs)
    |> Repo.insert()
  end

  def get_all(), do: Repo.all(Station)
  def get_by_id(id), do: Repo.get(Station, id)
  def remove(station), do: Repo.delete(station)

  @doc """
  Zwraca listę stacji o zadanych współrzędnych (dokładne porównanie `lon == , lat ==`):
  """
  def find_by_location(lon, lat) do
    Ecto.Query.from(s in Pollutiondb.Station,
      where: s.lon == ^lon and s.lat == ^lat
    )
    |> Repo.all()
  end

  @doc """
  Jeśli istnieje stacja o dokładnych współrzędnych (lat, lon), zwraca strukturę `%Station{}`.
  Jeśli nie – zwraca `nil`.
  """
  def get_by_coords(lat, lon) when is_float(lat) and is_float(lon) do
    case find_by_location(lon, lat) do
      [st | _] -> st
      [] -> nil
    end
  end

  def find_by_name(name) do
    Repo.all(Ecto.Query.where(Station, name: ^name))
  end

  def find_by_location_range(lon_min, lon_max, lat_min, lat_max) do
    Ecto.Query.from(s in Pollutiondb.Station,
      where: s.lon >= ^lon_min and s.lon <= ^lon_max,
      where: s.lat >= ^lat_min and s.lat <= ^lat_max
    )
    |> Repo.all()
  end

  def changeset(station, attrs) do
    station
    |> cast(attrs, [:name, :lon, :lat])
    |> validate_required([:name, :lon, :lat])
    |> validate_number(:lon, greater_than_or_equal_to: -180, less_than_or_equal_to: 180)
    |> validate_number(:lat, greater_than_or_equal_to: -90, less_than_or_equal_to: 90)
  end

  def update_name(station, new_name) do
    station
    |> changeset(%{name: new_name})
    |> Repo.update()
  end
end
