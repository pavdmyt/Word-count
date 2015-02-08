from nose.tools import assert_equal
from mapreduce import sanitize, Map, partition, Reduce


class Tests:

    def setUp(self):
        self.map_res = [('spam', 1), ('eggs', 1),
                        ('spam', 1), ('foo', 1)]
        self.part_res = partition(self.map_res)

    def test_sanitize(self):
        # trailing characters removal
        assert_equal(sanitize('spam,'), 'spam')
        assert_equal(sanitize('spam.'), 'spam')
        assert_equal(sanitize('spam!'), 'spam')
        assert_equal(sanitize('spam?'), 'spam')
        assert_equal(sanitize('spam1'), 'spam1')
        assert_equal(sanitize('spam!@#$%^&*()_+{}/'), 'spam')

        # leading characters removal
        assert_equal(sanitize('.spam'), 'spam')
        assert_equal(sanitize(',spam'), 'spam')
        assert_equal(sanitize('!spam'), 'spam')
        assert_equal(sanitize('?spam'), 'spam')
        assert_equal(sanitize('1spam'), '1spam')
        assert_equal(sanitize('!@#$%^&*()_+{}/spam'), 'spam')

        # both leading and trailing characters
        assert_equal(sanitize('.spam!'), 'spam')

    def test_Map(self):
        assert_equal(list(map(Map, ['spam'])), [('spam', 1)])
        assert_equal(list(map(Map, ['Spam'])), [('spam', 1)])
        assert_equal(list(map(Map, ['spam,'])), [('spam', 1)])
        assert_equal(list(map(Map, ['spam,', 'eggs'])),
                     [('spam', 1), ('eggs', 1)])

    def test_partition(self):
        assert_equal(partition(self.map_res),
                     {'spam': [('spam', 1), ('spam', 1)],
                      'eggs': [('eggs', 1)],
                      'foo':  [('foo', 1)]
                      })

    def test_Reduce(self):
        result = list(map(Reduce, self.part_res.items()))
        assert ('eggs', 1) in result
        assert ('spam', 2) in result
        assert ('foo', 1) in result
