import base64
import io
import zipfile


def generate_unique_pairs(data):
    """For a given list of items, yield a list of unique, size-sorted pairs"""
    sorted_data = sorted(data)
    for index_a, data_a in enumerate(sorted_data):
        for _, data_b in enumerate(sorted_data[index_a + 1 :]):
            yield data_a, data_b


def generate_unique_groups(data):
    """For a given list of items, yield a list of unique, size-sorted groups"""
    sorted_data = sorted(data)
    for index_a, data_a in enumerate(sorted_data):
        group_b = []
        for _, data_b in enumerate(sorted_data[index_a + 1 :]):
            group_b.append(data_b)
        if group_b:
            yield data_a, group_b


def occurrences(string, sub):
    # https://stackoverflow.com/a/2970542
    count = start = 0
    while True:
        start = string.find(sub, start) + 1
        if start > 0:
            count += 1
        else:
            return count


def generate_segmented_sequence(weights, sequence_length):
    sequence = []
    sum_of_weights = sum([value for value in weights.values()])
    for key, weight in sorted(weights.items(), key=lambda x: x[1]):
        num_entries = int(weight / sum_of_weights * sequence_length)
        for _ in range(num_entries):
            sequence.append(key)
    if weights and len(sequence) < sequence_length:
        sequence.append(key)
    return sequence


def compress_encode(content):
    zip_buffer = io.BytesIO()
    zip_base64 = ""
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr("content", content)
    zip_base64 = base64.b85encode(zip_buffer.getvalue()).decode()
    return zip_base64


# An export's function entries inflate to 3-12 times their encoded size: 4.1 times at the median and
# 11.6 at most over the 475 non-Intel samples of the live corpus (a CIL sample in which 94% of the
# functions share one pichash), 6.3 at most for Intel reports kept with their disassembly. Content
# inflating to many times more is no export but a decompression bomb. The ratio also bounds what
# json.loads makes of it, up to 24 bytes of Python objects per byte for [{},{},...], so it stays low:
# at 100x, a 0.5 MB POST /import still grew the server by 1.3 GB. A floor keeps small, repetitive
# samples from being refused for the ratio, as content that small is no danger either way.
MAX_DECOMPRESSION_RATIO = 20
MIN_DECOMPRESSED_SIZE = 8 * 1024 * 1024


def decompress_decode(compressed_b64):
    de_b64ed = base64.b85decode(compressed_b64)
    zip_buffer = io.BytesIO(de_b64ed)
    decompressed = ""
    max_size = max(MAX_DECOMPRESSION_RATIO * len(compressed_b64), MIN_DECOMPRESSED_SIZE)
    with zipfile.ZipFile(zip_buffer, "r", zipfile.ZIP_DEFLATED, False) as zip_file:
        content_info = zip_file.getinfo("content")
        # only what compress_encode writes: bzip2 and LZMA members are inflated in one piece, which no read size bounds
        if content_info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            raise ValueError("Compressed content must be deflated or stored.")
        if content_info.file_size > max_size:
            raise ValueError(f"Compressed content inflates to more than {MAX_DECOMPRESSION_RATIO} times its size and {MIN_DECOMPRESSED_SIZE} bytes.")
        with zip_file.open(content_info) as content_file:
            decompressed = content_file.read(max_size + 1)
    if len(decompressed) > max_size:
        raise ValueError(f"Compressed content inflates to more than {MAX_DECOMPRESSION_RATIO} times its size and {MIN_DECOMPRESSED_SIZE} bytes.")
    return decompressed


def encode_two_complement(unit64):
    if unit64 > 0x7FFFFFFFFFFFFFFF:
        return unit64 - 0x10000000000000000
    return unit64


def decode_two_complement(int64):
    if int64 < 0:
        return int64 + 0x10000000000000000
    return int64
