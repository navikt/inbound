from inbound.core.utils import generate_id


def test_generate_id():

    id = generate_id()

    assert id.count("-") == 2
