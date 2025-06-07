defmodule Pollutiondb.Repo.Migrations.CreateReadings do
  use Ecto.Migration

  def change do
    create table(:readings) do
      add :date, :date
      add :time, :time
      add :type, :string
      add :value, :float

      add :station_id, references(:stations, on_delete: :delete_all)

      timestamps()
    end

    create index(:readings, [:station_id])
  end
end
