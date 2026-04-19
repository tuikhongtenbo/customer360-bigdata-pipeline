from pyspark.sql.functions import lower, when, lit
from pyspark.sql import Column

CATEGORY_RULES = [
    (["vtv", "htv", "thvl", "kênh", "truyền hình", "channel"], "TV"),
    (["vs ", "bóng đá", "bong da", "world cup", "ngoại hạng", "premier league",
      "gia lai", "chelsea", "arsenal", "sea games", "sport"], "Sport"),
    (["tiếng anh", "tieng anh", "toán", "toan", "lớp ", "lop ", "learning"], "Learning"),
    (["doraemon", "tom and jerry", "tom & jerry", "peppa", "paw patrol",
      "spongebob", "oggy", "mickey", "looney"], "Cartoon"),
    (["conan", "naruto", "one piece", "dragon ball", "học viện anh hùng",
      "hoc vien anh hung", "demon slayer", "jujutsu", "boruto", "bleach",
      "chuyen sinh", "chuyển sinh", "sword art", "hunter x", "anime"], "Anime"),
    (["tây hành", "tay hanh", "đấu la", "dau la", "đấu phá", "dau pha",
      "vạn giới", "tiên nghịch", "thần ấn"], "CN Animation"),
    (["phàm nhân tu tiên", "thanh lạc", "vân tịch", "lộc đỉnh ký", "tam sinh",
      "cẩm tú", "trường an", "hoa thiên cốt", "thiên kim", "ngọc lau", "ngoc lau",
      "trinh sát", "pháp y", "hậu cung", "bật thầy", "ngọt ngào", "dữ quân",
      "du quan", "hoa mãn", "thiên long", "ỷ thiên", "thần điêu", "trần tình lệnh",
      "khánh dư niên", "lang điện hạ", "diên hy", "như ý", "chân hoàn",
      "trung quốc", "trung quoc", "hoa ngữ", "c-drama"], "C-Drama"),
    (["hàn quốc", "han quoc", "kim tae", "song hye", "lee min ho", "hyun bin",
      "crash landing", "squid game", "itaewon", "penthouse", "kdrama", "k-drama",
      "goblin", "boys over", "hospital playlist", "vincenzo"], "K-Drama"),
    (["kinh dị", "horror", "ma lai", "quỷ", "ma ám", "annabelle", "conjuring",
      "lời nguyền", "loi nguyen", "ám ảnh", "the nun", "insidious"], "Horror movie"),
    (["hài", "comedy", "tươi cười", "phấn khởi", "vui nhộn"], "Comedy"),
    (["hoạt hình", "animation", "pixar", "disney", "dreamworks", "chú vẹt",
      "chu vet", "minions", "frozen", "kung fu panda", "đuôi dài"], "Animation movie"),
    (["hành động", "hanh dong", "action", "vượt ngục", "vuot nguc", "fast furious",
      "avengers", "marvel", "john wick", "khủng long", "hoang dã", "jurassic",
      "thế giới khủng", "tiếng gọi"], "Action movie"),
    (["yêu", "tình", "romantic", "love", "đích thực", "người yêu", "nguoi yeu",
      "siêu cấp", "định mệnh", "dinh menh", "vẻ đẹp", "ve dep",
      "high school king", "cô gái", "chàng trai"], "Romantic movie"),
]


def map_category(keyword_col: Column) -> Column:
    kw = lower(keyword_col)
    expr = lit("Other")
    for patterns, category in reversed(CATEGORY_RULES):
        condition = kw.contains(patterns[0])
        for p in patterns[1:]:
            condition = condition | kw.contains(p)
        expr = when(condition, lit(category)).otherwise(expr)
    return expr
