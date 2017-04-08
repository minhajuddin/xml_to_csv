defmodule XmlToCsv do
  import Logger, only: [info: 1, error: 1]
  import SweetXml

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
      process_file(file, index, parse_schema(schema_file))
    end, max_concurrency: System.schedulers_online, timeout: :infinity)
    |> Enum.to_list
  end

  defp process_file(file, index, schema) do
    info "#{index} processing #{file}"
    file
    |> File.read!
    |> parse
    |> transform_to_csv_row(schema)
    |> CSV.encode
    |> Enum.into(File.stream!("#{file}.csv"))
    info "#{index} finished processing #{file}"
    :ok
  end

  defp transform_to_csv_row(xml, {row_xpath, col_xpaths}) do
    xpath(xml, row_xpath) # get rows
    |> Enum.map(fn row ->
      col_xpaths |> Enum.map(fn col_xpath -> xpath(row, col_xpath) end)
    end)
  end

  defp parse_schema(schema_file) do
    [row_xpath | col_xpaths] = File.stream!(schema_file) |> Enum.map(&String.trim/1)
    {~x[#{row_xpath}]l, col_xpaths |> Enum.map(& ~x[#{&1}])}
  end
end
