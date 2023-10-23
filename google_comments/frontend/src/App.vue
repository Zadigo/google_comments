<template>
  <div class="container">
    <div class="row">
      <div class="col-12">
        <input v-model="url" type="url">
        <button type="button" @click="handleScrap">Press</button>
      </div>
    </div>
  </div>
</template>

<script>

export default {
  name: 'App',
  data () {
    return {
      ws: null,
      url: 'https://www.google.com/maps/search/boulangerie+louise/@50.6476038,3.0751393,12z/data=!3m1!4b1?entry=ttu'
    }
  },
  components: {
  },
  mounted() {
    this.handleWebsocket()
  },
  methods: {
    handleWebsocket () {
      const ws = new WebSocket('ws://127.0.0.1:5467/ws/start')
      ws.onopen = (e) => {
        // this.ws.send(JSON.stringify({ 'method': 'scrap', 'url': this.url }))
        console.log(e)
      }
      ws.onmessage = (e) => {
        console.log(e)
      }
      this.ws = ws
    },
    handleScrap () {
      this.ws.send(JSON.stringify({ 'method': 'scrap', 'url': this.url }))
    }
  }
}
</script>

<style>
</style>
