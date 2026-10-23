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


# base64.b85encode holds a list entry per 4 input bytes, about 40 times the input: encoded a slice at a time (a
# multiple of 4 bytes, so the result is the same), it holds that for one slice only
B85_SLICE_SIZE = 1 << 18


def b85encode_sliced(data) -> str:
    view = memoryview(data)
    return b"".join(base64.b85encode(view[start : start + B85_SLICE_SIZE]) for start in range(0, len(view), B85_SLICE_SIZE)).decode()


def compress_encode_fragments(fragments):
    """compress_encode of the UTF-8 text the byte fragments join to, compressed as they come instead of joined first."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        with zip_file.open("content", "w") as content:
            for fragment in fragments:
                content.write(fragment)
    return b85encode_sliced(zip_buffer.getvalue())


def compress_encode(content):
    zip_buffer = io.BytesIO()
    zip_base64 = ""
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr("content", content)
    zip_base64 = b85encode_sliced(zip_buffer.getvalue())
    return zip_base64


def decompress_decode(compressed_b64):
    de_b64ed = base64.b85decode(compressed_b64)
    zip_buffer = io.BytesIO(de_b64ed)
    decompressed = ""
    with zipfile.ZipFile(zip_buffer, "r", zipfile.ZIP_DEFLATED, False) as zip_file:
        decompressed = zip_file.read("content")
    return decompressed


def encode_two_complement(unit64):
    if unit64 > 0x7FFFFFFFFFFFFFFF:
        return unit64 - 0x10000000000000000
    return unit64


def decode_two_complement(int64):
    if int64 < 0:
        return int64 + 0x10000000000000000
    return int64
