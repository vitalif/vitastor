{
  'targets': [
    {
      'target_name': 'addon',
      'sources': [
        'client.cc',
        'addon.cc'
      ],
      'include_dirs': [
        '<!(node -e "require(\'nan\')")'
      ],
      'cflags': [
        '<!(pkg-config --cflags vitastor)'
      ],
      'libraries': [
        '<!(pkg-config --libs vitastor)',
        '-lvitastor_kv'
      ]
    }
  ]
}
