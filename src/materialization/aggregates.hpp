#ifndef AGGREGATES_H
#define AGGREGATES_H


namespace duckdb {
    struct Range {
        uint64_t min;
        uint64_t max;

        explicit Range(): min(UINT64_MAX), max(0) {
        }
    };

    template<typename T>
    void UpdateRange(const Vector &vector, const idx_t count, Range &range) {
        const auto data = FlatVector::GetData<T>(vector);

        uint64_t local_min = range.min;
        uint64_t local_max = range.max;

        idx_t i = 0;

        // Process in blocks of 512
        for (idx_t j = 0; j < count; j++) {
            const auto value = data[i + j];
            const uint64_t uvalue = static_cast<uint64_t>(value);

            // These two operations can be vectorized separately
            if (uvalue < local_min) {
                local_min = uvalue;
            }
            if (uvalue > local_max) {
                local_max = uvalue;
            }
        }


        // Write back to shared range
        range.min = local_min;
        range.max = local_max;
    }


    inline void UpdateRange(const Vector &vector, const idx_t count, Range &range) {
        switch (vector.GetType().id()) {
            case LogicalTypeId::BIGINT:
                return UpdateRange<int64_t>(vector, count, range);
            case LogicalTypeId::UBIGINT:
                return UpdateRange<uint64_t>(vector, count, range);
            case LogicalTypeId::INTEGER:
                return UpdateRange<int32_t>(vector, count, range);
            case LogicalTypeId::UINTEGER:
                return UpdateRange<uint32_t>(vector, count, range);
            case LogicalTypeId::SMALLINT:
                return UpdateRange<int16_t>(vector, count, range);
            case LogicalTypeId::USMALLINT:
                return UpdateRange<uint16_t>(vector, count, range);
            case LogicalTypeId::TINYINT:
                return UpdateRange<int8_t>(vector, count, range);
            case LogicalTypeId::UTINYINT:
                return UpdateRange<uint8_t>(vector, count, range);
            case LogicalTypeId::FLOAT:
                return UpdateRange<float>(vector, count, range);
            case LogicalTypeId::DOUBLE:
                return UpdateRange<double>(vector, count, range);
            default:
                throw NotImplementedException("Range not implemented for type: " + vector.GetType().ToString());
        }
    }
}

#endif
