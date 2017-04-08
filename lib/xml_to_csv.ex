defmodule XmlToCsv do
  import Logger, only: [info: 1, error: 1]

  def main([]) do
    error """
    USAGE:
      xml_to_csv schema_file_path file1.xml file2.xml ...
    """
  end
  def main([schema_file | files]) do
    files
    |> Enum.with_index(1)
    |> Task.async_stream(fn {file, index} ->
      process_file(file, index, schema)
    end, max_concurrency: System.schedulers_online, timeout: :infinity)
  end

  defp process_file(file, index, schema) do
    import SweetXml
    info "#{index} processing #{file}"
    file
    |> File.read!
    |> parse
    |> transform_to_csv_row(schema)
    |> CSV.encode
    |> Enum.into(File.stream!("#{file}.csv"))
    info "#{index} finished processing #{file}"
  end

  defp transform_to_csv_row(xml, schema) do
  end
end
