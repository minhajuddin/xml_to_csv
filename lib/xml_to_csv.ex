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
    Process.flag(:trap_exit, true)
    bad_files = :ets.new(:bad_files, [{:write_concurrency, true}, :public])
    files
    |> Enum.with_index(1)
    |> Task.async_stream(fn {file, index} ->
      try do
        process_file(file, index, parse_schema(schema_file))
      rescue
        err ->
          Logger.error("ERROR: #{file}, #{inspect err}")
          :ets.insert(bad_files, {file, err})
      catch
        :exit, err ->
          Logger.error("ERROR: #{file}, #{inspect err}")
          :ets.insert(bad_files, {file, err})
      end
    end, max_concurrency: System.schedulers_online, timeout: :infinity)
    |> Enum.to_list

    flush_messages()

    case :ets.tab2list(bad_files) do
      [] -> :ok
      bad_files ->
        error "The following files failed to be processed"
        bad_files
        |> Enum.with_index(1)
        |> Enum.each(fn {{file, _err}, index} ->
          error "#{index}. #{file}"
        end)
        exit({:shutdown, -1})
    end
  end

  defp flush_messages do
    receive do
      {:EXIT, _pid, :normal} -> flush_messages()
      {:EXIT, _pid, reason} ->
        Logger.error("ERROR: #{inspect reason}")
        flush_messages()
    after 0 ->
      :ok
    end
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
    {~x[#{row_xpath}]l, col_xpaths |> Enum.map(& ~x[#{&1}]o)}
  end
end
