/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/Hive/HiveDataPart.h>
#include <Storages/StorageCnchHive.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include <arrow/adapters/orc/adapter.h>


namespace DB
{

template <class FieldType, class StatisticsType>
Range createRangeFromParquetStatistics(std::shared_ptr<StatisticsType> stats)
{
    ///
    if (!stats->HasMinMax())
        return Range();
    return Range(FieldType(stats->min()), true, FieldType(stats->max()), true);
}

Range createRangeFromParquetStatistics(std::shared_ptr<parquet::ByteArrayStatistics> stats)
{
    if (!stats->HasMinMax())
        return Range();

    String min_val(reinterpret_cast<const char *>(stats->min().ptr), stats->min().len);
    String max_val(reinterpret_cast<const char *>(stats->max().ptr), stats->max().len);
    return Range(min_val, true, max_val, true);
}

HiveDataPart::HiveDataPart(
    const String & name_,
    const String & hdfs_uri_,
    const String & relative_path_,
    const DiskPtr & disk_,
    const HivePartInfo & info_,
    const String & format_name_,
    std::unordered_set<Int64> skip_splits_,
    NamesAndTypesList index_names_and_types_)
    : name(name_)
    , hdfs_uri(hdfs_uri_)
    , relative_path(relative_path_)
    , disk(disk_)
    , info(info_)
    , format_name(format_name_)
    , skip_splits(skip_splits_)
    , index_names_and_types(index_names_and_types_)
{
}

String HiveDataPart::getFullDataPartPath() const
{
    return relative_path + "/" + name;
}

String HiveDataPart::getFullTablePath() const
{
    return relative_path;
}

String HiveDataPart::getHDFSUri() const
{
    return hdfs_uri;
}

String HiveDataPart::getFormatName() const
{
    return format_name;
}

HiveDataPart::~HiveDataPart() = default;

void HiveDataPart::loadSplitMinMaxIndexes()
{
    if (split_minmax_idxes_loaded)
        return;
    loadSplitMinMaxIndexesImpl();
    split_minmax_idxes_loaded = true;
}

arrow::Status HiveDataPart::tryGetTotalRowGroups(size_t & res) const
{
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hive disk is not set");

    std::unique_ptr<ReadBuffer> in = disk->readFile(getFullDataPartPath());
    std::unique_ptr<parquet::arrow::FileReader> reader = nullptr;
    arrow::Status read_status = parquet::arrow::OpenFile(asArrowFile(*in), arrow::default_memory_pool(), &reader);
    if (read_status.ok())
    {
        auto parquet_meta = reader->parquet_reader()->metadata();
        if (parquet_meta)
            res = parquet_meta->num_row_groups();
        else
            res = 0;
    }
    else
        res = 0;

    return read_status;
}

size_t HiveDataPart::getTotalRowGroups() const
{
    if (total_row_groups != 0)
        return total_row_groups;

    size_t res = 0;
    arrow::Status read_status = tryGetTotalRowGroups(res);
    if (!read_status.ok())
        throw Exception("Unexpected error of getTotalRowGroups. because of meta is NULL", ErrorCodes::LOGICAL_ERROR);

    total_row_groups = res;

    LOG_TRACE(&Poco::Logger::get("HiveDataPart"), " num_row_groups: {} total_row_groups: {}", res, total_row_groups);

    return res;
}

arrow::Status HiveDataPart::tryGetTotalStripes(size_t & res) const
{
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hive disk is not set");

    std::unique_ptr<ReadBuffer> in = disk->readFile(getFullDataPartPath());
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader = nullptr;
    arrow::Status read_status = arrow::adapters::orc::ORCFileReader::Open(asArrowFile(*in), arrow::default_memory_pool(), &reader);
    if (read_status.ok())
        res = reader->NumberOfStripes();
    else
        res = 0;

    return read_status;
}

size_t HiveDataPart::getTotalStripes() const
{
    if (total_stripes != 0)
        return total_stripes;

    size_t res = 0;
    arrow::Status read_status = tryGetTotalStripes(res);
    if (!read_status.ok())
        throw Exception("Unexpected error of getTotalStripes. because of meta is NULL", ErrorCodes::LOGICAL_ERROR);

    total_stripes = res;

    LOG_TRACE(&Poco::Logger::get("HiveDataPart"), "res = {} total_stripes = {}", res, total_stripes);

    return res;
}

size_t HiveDataPart::getTotalBlockNumber() const
{
    size_t res = 0;
    if (toFileFormat(format_name) == FileFormat::ORC)
        res = getTotalStripes();
    else if (toFileFormat(format_name) == FileFormat::PARQUET)
        res = getTotalRowGroups();
    else
        throw Exception("Unexpected Format in CnchHive ,currently only support Parquet/orc", ErrorCodes::LOGICAL_ERROR);

    return res;
}

void HiveDataPart::loadSplitMinMaxIndexesImpl()
{
    if (toFileFormat(format_name) == FileFormat::ORC)
        loadOrcSplitMinMaxIndexesImpl();
    else if (toFileFormat(format_name) == FileFormat::PARQUET)
        loadParquetSplitMinMaxIndexesImpl();
    else
        throw Exception("Unexpected Format in CnchHive ,currently only support Parquet/orc", ErrorCodes::LOGICAL_ERROR);
}

void HiveDataPart::loadOrcSplitMinMaxIndexesImpl()
{
    /// TODO:
}

/// use arrow library, loop parquet datapart row groups
/// build requried column min、max range.
/// currently only support basic data type
void HiveDataPart::loadParquetSplitMinMaxIndexesImpl()
{
    /// TODO:
}

}
