import datetime
import hashlib
from typing import Any, Dict


def wrap_string(input_str, max_column_length=100, padding=0):
    words = input_str.split()
    lines = []
    current_line = words[0]

    for word in words[1:]:
        if len(current_line) + 1 + len(word) <= max_column_length:
            current_line += " " + word
        else:
            lines.append(current_line)
            current_line = word

    lines.append(current_line)
    join_char = "\n" + padding * " "
    formatted_output = join_char.join(lines)

    return formatted_output


# Dataclass, post init
# constructor -> .fromDict


class UniqueBlocksResult:
    statistics: dict
    unique_blocks: dict
    yara_rule: list

    def __init__(self) -> None:
        pass

    def generateYaraRule(self, min_ins=None, max_ins=None, min_bytes=None, max_bytes=None, required_per_sample=10, condition_required=7, wrap_at=80):
        cover = self.generateBlockCover(min_ins=min_ins, max_ins=max_ins, min_bytes=min_bytes, max_bytes=max_bytes, required_per_sample=required_per_sample)
        return self.renderRule(cover, condition_required, wrap_at=wrap_at)

    def renderRule(self, block_cover, condition_required, wrap_at=80):
        """Render the YARA rule. wrap_at is the column to wrap the hex at; 0 keeps it on one line."""
        yara_blocks = block_cover["block_hashes"]
        block_hash_string = ",".join([pic_hash for pic_hash in yara_blocks])
        rule_identifier = hashlib.sha256(block_hash_string.encode()).hexdigest()[:16]
        yara_rule = f"rule mcrit_{rule_identifier} {{\n"
        yara_rule += "    meta:\n"
        yara_rule += '        author = "MCRIT YARA Generator"\n'
        yara_rule += '        description = "Code-based YARA rule composed from potentially unique basic blocks for the selected set of samples/family."\n'
        rule_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
        yara_rule += f'        date = "{rule_date}"\n'
        yara_rule += "    strings:\n"
        yara_rule += (
            f"        // Rule generation selected {len(yara_blocks)} picblocks, covering {block_cover['num_samples_covered']}/{self.statistics['num_samples']} input sample(s).\n"
        )
        for pichash, result in self.unique_blocks.items():
            if pichash not in yara_blocks:
                continue
            yarafied = f"        /* picblockhash: {pichash} - coverage: {len(result['samples'])}/{block_cover['num_samples_covered']} samples.\n"
            maxlen_ins = max([len(ins[1]) for ins in result["instructions"]])
            for ins in result["instructions"]:
                yarafied += f"         * {ins[1]:{maxlen_ins}} | {ins[2]} {ins[3]}\n"
            yarafied += "         */\n"
            if wrap_at:
                yarafied += f"        $blockhash_{pichash} = {{\n"
                yarafied += "            " + wrap_string(result["escaped_sequence"], max_column_length=wrap_at, padding=12) + "\n"
                yarafied += "        }\n"
            else:
                yarafied += f"        $blockhash_{pichash} = {{ " + result["escaped_sequence"] + " }\n"
            yara_rule += yarafied + "\n"
        yara_rule += "    condition:\n"
        yara_rule += f"        {min(len(yara_blocks), condition_required)} of them\n"
        yara_rule += "}\n"
        return yara_rule

    def generateBlockCover(self, min_ins=None, max_ins=None, min_bytes=None, max_bytes=None, required_per_sample=10):
        block_cover: Dict[str, Any] = {"block_hashes": [], "num_samples_covered": 0, "has_rule": False, "is_complete_cover": False}
        # we need to filter first, according to the desired parameters
        filtered_blocks: Dict[int, Dict[str, Any]] = {}
        for block_hash, entry in self.unique_blocks.items():
            # no disassembly stored for its function: nothing to write a pattern from
            if not entry.get("instructions"):
                continue
            if min_ins and entry["length"] < min_ins:
                continue
            if max_ins and entry["length"] > max_ins:
                continue
            bytes_length = len(entry["escaped_sequence"].replace(" ", "")) // 2
            if min_bytes and bytes_length < min_bytes:
                continue
            if max_bytes and bytes_length > max_bytes:
                continue
            filtered_blocks[block_hash] = entry
        yara_rule_blocks = []
        sample_ids = [int(v) for v in self.statistics["by_sample_id"].keys()]
        sample_coverage = {sample_id: 0 for sample_id in sample_ids}
        samples_covered = set()
        while True:
            # calculate block_scores as how much benefit they bring, i.e. how many uncovered samples they can cover at once
            block_candidates = []
            for block_hash, entry in filtered_blocks.items():
                sample_ids_coverable = set(entry["samples"]).difference(samples_covered)
                if sample_ids_coverable and block_hash not in yara_rule_blocks:
                    candidate = {"block_hash": block_hash, "coverable": sample_ids_coverable, "value": len(sample_ids_coverable), "score": entry["score"]}
                    block_candidates.append(candidate)
            # check if we are done yet, successful or not
            if len(samples_covered) == len(sample_ids):
                block_cover["has_rule"] = True
                block_cover["is_complete_cover"] = True
                break
            if len(block_candidates) == 0:
                if len(yara_rule_blocks) > 0:
                    block_cover["has_rule"] = True
                break
            # if not, choose the best block
            block_candidates.sort(key=lambda i: (i["value"], i["score"]))
            selected_block: Dict[str, Any] = block_candidates.pop()
            yara_rule_blocks.append(selected_block["block_hash"])
            # and update counters
            for sample_id in selected_block["coverable"]:
                sample_coverage[sample_id] += 1
            samples_covered = set([sample_id for sample_id, count in sample_coverage.items() if count >= required_per_sample])
            block_cover["num_samples_covered"] = len(samples_covered)
        block_cover["block_hashes"] = yara_rule_blocks
        return block_cover

    def toDict(self):
        # rebuild the original UniqueBlocks result here
        blocks_dict = {
            "statistics": self.statistics,
            "unique_blocks": self.unique_blocks,
            "yara_rule": self.yara_rule,
        }
        return blocks_dict

    @classmethod
    def fromDict(cls, entry_dict):
        blocks_result = cls()
        blocks_result.statistics = entry_dict["statistics"]
        blocks_result.unique_blocks = entry_dict["unique_blocks"]
        blocks_result.yara_rule = entry_dict["yara_rule"]
        return blocks_result

    def __str__(self):
        if self.statistics is not None:
            return "UniqueBlocksResult: {} Samples with {} unique blocks.".format(len(self.statistics["by_sample_id"]), len(self.unique_blocks))
        else:
            return "UniqueBlocksResult: nothing parsed."
